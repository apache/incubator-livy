#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import os
import sys
import base64
import json
import time
try:
    from urllib.parse import urlparse
except ImportError:
     from urlparse import urlparse
import requests
from requests_kerberos import HTTPKerberosAuth, REQUIRED, OPTIONAL
import cloudpickle
import pytest

# Print environment info for debugging
print(f"=== Python Environment ===")
print(f"Python version: {sys.version}")
print(f"Python executable: {sys.executable}")
print(f"cloudpickle version: {cloudpickle.__version__}")
print(f"==========================")
try:
    import httplib
except ImportError:
    from http import HTTPStatus as httplib
from flaky import flaky

global session_id, job_id
session_id = None
job_id = None

livy_end_point = os.environ.get("LIVY_END_POINT")
auth_scheme = os.environ.get("AUTH_SCHEME", "")
user = os.environ.get("USER", "")
password = os.environ.get("PASSWORD", "")
ssl_cert = os.environ.get("SSL_CERT", True)

if auth_scheme == 'kerberos':
    request_auth = HTTPKerberosAuth(mutual_authentication=OPTIONAL, sanitize_mutual_error_response=False, force_preemptive = True)
elif auth_scheme == 'basic':
    request_auth = (user, password)
else:
    request_auth = None

add_file_url = os.environ.get("ADD_FILE_URL")
add_pyfile_url = os.environ.get("ADD_PYFILE_URL")
upload_file_url = os.environ.get("UPLOAD_FILE_URL")
upload_pyfile_url = os.environ.get("UPLOAD_PYFILE_URL")

@pytest.fixture(scope="module", autouse=True)
def after_all(request):
    request.addfinalizer(stop_session)


def process_job(job, expected_result, is_error_job=False):
    global job_id

    pickled_job = cloudpickle.dumps(job)
    base64_pickled_job = base64.b64encode(pickled_job).decode('utf-8')
    base64_pickled_job_json = json.dumps({'job': base64_pickled_job, 'jobType': 'pyspark'})
    request_url = livy_end_point + "/sessions/" + str(session_id) + "/submit-job"
    header = {'Content-Type': 'application/json', 'X-Requested-By': 'livy'}
    response = requests.request('POST', request_url, headers=header, data=base64_pickled_job_json, auth=request_auth, verify=ssl_cert)

    assert response.status_code == httplib.CREATED
    job_id = response.json()['id']

    poll_time = 1
    max_poll_time = 30
    poll_response = None
    while (poll_response is None or poll_response.json()['state'] == 'STARTED') and poll_time < \
            max_poll_time:
        time.sleep(poll_time)
        poll_request_uri = livy_end_point + "/sessions/" + str(session_id) + \
                           "/jobs/" + str(job_id)
        poll_header = {'X-Requested-By': 'livy'}
        poll_response = requests.request('GET', poll_request_uri, headers=poll_header, auth=request_auth, verify=ssl_cert)
        poll_time *= 2

    assert poll_response.json()['id'] == job_id
    assert poll_response.status_code == httplib.OK
    if not is_error_job:
        assert poll_response.json()['error'] is None
        result = poll_response.json()['result']
        b64_decoded = base64.b64decode(result)
        b64_decoded_decoded = base64.b64decode(b64_decoded)
        deserialized_object = cloudpickle.loads(b64_decoded_decoded)
        assert deserialized_object == expected_result
    else:
        error = poll_response.json()['error']
        assert expected_result in error


def delay_rerun(*args):
    time.sleep(10)
    return True


def stop_session():
    global session_id

    request_url = livy_end_point + "/sessions/" + str(session_id)
    headers = {'X-Requested-By': 'livy'}
    response = requests.request('DELETE', request_url, headers=headers, auth=request_auth, verify=ssl_cert)
    assert response.status_code == httplib.OK


def test_create_session():
    global session_id

    request_url = livy_end_point + "/sessions"
    uri = urlparse(request_url)
    header = {'Content-Type': 'application/json', 'X-Requested-By': 'livy'}
    json_data = json.dumps({'kind': 'pyspark', 'conf': {'livy.uri': uri.geturl()}})
    response = requests.request('POST', request_url, headers=header, data=json_data, auth=request_auth, verify=ssl_cert)

    assert response.status_code == httplib.CREATED
    session_id = response.json()['id']


def get_yarn_rm_base_url(spark_ui_url):
    """Extract YARN RM base URL from sparkUiUrl."""
    if not spark_ui_url:
        return None

    from urllib.parse import urlparse
    parsed = urlparse(spark_ui_url)
    return f"http://{parsed.netloc}"


def get_yarn_attempt_logs(app_id, rm_base_url):
    """Fetch logs for each application attempt from YARN."""
    if not app_id or not rm_base_url:
        return f"(missing app_id={app_id} or rm_base_url={rm_base_url})"

    output = []
    try:
        # Get all attempts for this application
        attempts_url = f"{rm_base_url}/ws/v1/cluster/apps/{app_id}/appattempts"
        output.append(f"Fetching attempts from: {attempts_url}")

        resp = requests.get(attempts_url, timeout=10)
        if resp.status_code != 200:
            return f"(failed to get attempts: {resp.status_code} - {resp.text[:500]})"

        attempts_data = resp.json()
        attempts = attempts_data.get('appAttempts', {}).get('appAttempt', [])

        if not attempts:
            output.append("No attempts found")
            return "\n".join(output)

        output.append(f"Found {len(attempts)} attempt(s)")

        for attempt in attempts:
            attempt_id = attempt.get('appAttemptId', 'unknown')
            container_id = attempt.get('containerId', 'unknown')
            node_id = attempt.get('nodeId', 'unknown')
            logs_link = attempt.get('logsLink', '')

            output.append(f"\n--- Attempt: {attempt_id} ---")
            output.append(f"Container: {container_id}")
            output.append(f"Node: {node_id}")
            output.append(f"State: {attempt.get('appAttemptState', 'unknown')}")
            output.append(f"Diagnostics: {attempt.get('diagnostics', 'none')}")

            # Try to fetch the actual container logs
            if logs_link:
                output.append(f"Logs URL: {logs_link}")
                try:
                    logs_resp = requests.get(logs_link, timeout=30)
                    if logs_resp.status_code == 200:
                        # Extract text from HTML (rough extraction)
                        log_text = logs_resp.text
                        # Try to find stderr/stdout sections
                        output.append(f"Container logs (last 5000 chars):")
                        output.append(log_text[-5000:])
                    else:
                        output.append(f"(logs fetch failed: {logs_resp.status_code})")
                except Exception as e:
                    output.append(f"(failed to fetch logs: {e})")
            else:
                output.append("(no logsLink available)")

        return "\n".join(output)

    except Exception as e:
        return f"(failed to get attempt logs: {e})"


def get_yarn_app_info(app_id, rm_base_url):
    """Fetch YARN application info via REST API."""
    if not app_id or not rm_base_url:
        return f"(missing app_id={app_id} or rm_base_url={rm_base_url})"
    try:
        app_url = f"{rm_base_url}/ws/v1/cluster/apps/{app_id}"
        resp = requests.get(app_url, timeout=10)
        if resp.status_code == 200:
            app_info = resp.json().get('app', {})
            return json.dumps({
                'state': app_info.get('state'),
                'finalStatus': app_info.get('finalStatus'),
                'diagnostics': app_info.get('diagnostics'),
                'amContainerLogs': app_info.get('amContainerLogs'),
            }, indent=2)
        else:
            return f"(YARN API returned {resp.status_code}: {resp.text[:500]})"
    except Exception as e:
        return f"(failed to get YARN app info: {e})"


@flaky(max_runs=6, rerun_filter=delay_rerun)
def test_wait_for_session_to_become_idle():
    request_url = livy_end_point + "/sessions/" + str(session_id)
    header = {'X-Requested-By': 'livy'}
    response = requests.request('GET', request_url, headers=header, auth=request_auth, verify=ssl_cert)
    assert response.status_code == httplib.OK
    session_data = response.json()
    session_state = session_data['state']

    # Print session details if not idle for debugging
    if session_state != 'idle':
        print(f"Session state: {session_state}")
        print(f"Session data: {json.dumps(session_data, indent=2)}")
        # Try to get session log
        log_url = livy_end_point + "/sessions/" + str(session_id) + "/log"
        log_resp = requests.request('GET', log_url, headers=header, auth=request_auth, verify=ssl_cert)
        if log_resp.status_code == httplib.OK:
            print(f"Session log: {json.dumps(log_resp.json(), indent=2)}")

        # If session is dead, fetch YARN attempt logs for root cause
        if session_state == 'dead':
            app_id = session_data.get('appId')
            spark_ui_url = session_data.get('appInfo', {}).get('sparkUiUrl')
            rm_base_url = get_yarn_rm_base_url(spark_ui_url)

            print(f"=== YARN Application Info for {app_id} ===")
            print(f"sparkUiUrl: {spark_ui_url}")
            print(f"RM base URL: {rm_base_url}")
            print(get_yarn_app_info(app_id, rm_base_url))

            print(f"=== YARN Attempt Logs ===")
            print(get_yarn_attempt_logs(app_id, rm_base_url))
            print("=== End YARN Info ===")

    assert session_state == 'idle'


def test_spark_job():
    def simple_spark_job(context):
        elements = [10, 20, 30]
        sc = context.sc
        return sc.parallelize(elements, 2).count()

    process_job(simple_spark_job, 3)


def test_error_job():
    def error_job(context):
        return "hello" + 1

    process_job(error_job,
        "TypeError: ", True)


def test_reconnect():
    global session_id

    request_url = livy_end_point + "/sessions/" + str(session_id) + "/connect"
    header = {'Content-Type': 'application/json', 'X-Requested-By': 'livy'}
    response = requests.request('POST', request_url, headers=header, auth=request_auth, verify=ssl_cert)

    assert response.status_code == httplib.OK
    assert session_id == response.json()['id']


def test_add_file():
    add_file_name = os.path.basename(add_file_url)
    json_data = json.dumps({'uri': add_file_url})
    request_url = livy_end_point + "/sessions/" + str(session_id) + "/add-file"
    header = {'Content-Type': 'application/json', 'X-Requested-By': 'livy'}
    response = requests.request('POST', request_url, headers=header, data=json_data, auth=request_auth, verify=ssl_cert)

    assert response.status_code == httplib.OK

    def add_file_job(context):
        from pyspark import SparkFiles
        with open(SparkFiles.get(add_file_name)) as testFile:
            file_val = testFile.readline()
        return file_val

    process_job(add_file_job, "hello from addfile")


def test_add_pyfile():
    add_pyfile_name_with_ext = os.path.basename(add_pyfile_url)
    add_pyfile_name = add_pyfile_name_with_ext.rsplit('.', 1)[0]
    json_data = json.dumps({'uri': add_pyfile_url})
    request_url = livy_end_point + "/sessions/" + str(session_id) + "/add-pyfile"
    header = {'Content-Type': 'application/json', 'X-Requested-By': 'livy'}
    response_add_pyfile = requests.request('POST', request_url, headers=header, data=json_data, auth=request_auth, verify=ssl_cert)

    assert response_add_pyfile.status_code == httplib.OK

    def add_pyfile_job(context):
       pyfile_module = __import__ (add_pyfile_name)
       return pyfile_module.test_add_pyfile()

    process_job(add_pyfile_job, "hello from addpyfile")


def test_upload_file():
    upload_file = open(upload_file_url)
    upload_file_name = os.path.basename(upload_file.name)
    request_url = livy_end_point + "/sessions/" + str(session_id) + "/upload-file"
    files = {'file': upload_file}
    header = {'X-Requested-By': 'livy'}
    response = requests.request('POST', request_url, headers=header, files=files, auth=request_auth, verify=ssl_cert)

    assert response.status_code == httplib.OK

    def upload_file_job(context):
        from pyspark import SparkFiles
        with open(SparkFiles.get(upload_file_name)) as testFile:
            file_val = testFile.readline()
        return file_val

    process_job(upload_file_job, "hello from uploadfile")


def test_upload_pyfile():
    upload_pyfile = open(upload_pyfile_url)
    upload_pyfile_name_with_ext = os.path.basename(upload_pyfile.name)
    upload_pyfile_name = upload_pyfile_name_with_ext.rsplit('.', 1)[0]
    request_url = livy_end_point + "/sessions/" + str(session_id) + "/upload-pyfile"
    files = {'file': upload_pyfile}
    header = {'X-Requested-By': 'livy'}
    response = requests.request('POST', request_url, headers=header, files=files, auth=request_auth, verify=ssl_cert)
    assert response.status_code == httplib.OK

    def upload_pyfile_job(context):
        pyfile_module = __import__ (upload_pyfile_name)
        return pyfile_module.test_upload_pyfile()
    process_job(upload_pyfile_job, "hello from uploadpyfile")


if __name__ == '__main__':
    value = pytest.main([os.path.dirname(__file__)])
    if value != 0:
        raise Exception("One or more test cases have failed.")
