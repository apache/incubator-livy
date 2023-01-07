#!/bin/bash
#
# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Generates a CA certificate, a server key, and a server certificate signed by the CA.


# Idea taken from here:
# https://github.com/GoogleCloudPlatform/spark-on-k8s-operator/blob/spark-operator-chart-1.0.8/hack/gencerts.sh


set -e
SCRIPT=`basename ${BASH_SOURCE[0]}`

function usage {
  cat<< EOF
  Usage: $SCRIPT
  Options:
  -h | --help                    Display help information.
  -n | --namespace <namespace>   The namespace where Livy is installed.
  -s | --service <service>       The name of the service.
  -e | --secret <secret>         The name of the secret to generate.
  -p | --in-pod                  Whether the script is running inside a pod or not.
  -o | --owner <statefulset>     Use <statefulset> in ownerReferences.
EOF
}

function parse_arguments {
  while [[ $# -gt 0 ]]
  do
    case "$1" in
      -n|--namespace)
      if [[ -n "$2" ]]; then
        NAMESPACE="$2"
      else
        echo "-n or --namespace requires a value."
        exit 1
      fi
      shift 2
      continue
      ;;
      -s|--service)
      if [[ -n "$2" ]]; then
        SERVICE="$2"
      else
        echo "-s or --service requires a value."
        exit 1
      fi
      shift 2
      continue
      ;;
      -e|--secret)
      if [[ -n "$2" ]]; then
        SECRET="$2"
      else
        echo "-e or --secret requires a value."
        exit 1
      fi
      shift 2
      continue
      ;;
      -p|--in-pod)
      export IN_POD=true
      shift 1
      continue
      ;;
      -o|--owner)
      if [[ -n "$2" ]]; then
        OWNER="$2"
      else
        echo "-o or --owner requires a value."
        exit 1
      fi
      shift 2
      continue
      ;;
      -h|--help)
      usage
      exit 0
      ;;
      --)              # End of all options.
        shift
        break
      ;;
      '')              # End of all options.
        break
      ;;
      *)
        echo "Unrecognized option: $1"
        exit 1
      ;;
    esac
  done
}

# Set the defaults.
IN_POD=false
SERVICE="livy-http"
NAMESPACE="default"
SECRET="livy-certs"
STOREPASS="password123"
parse_arguments "$@"

TMP_DIR="/tmp/certs"

echo "Generating certs in ${TMP_DIR}."
mkdir -p ${TMP_DIR}
cat > ${TMP_DIR}/server.conf << EOF
[req]
req_extensions = v3_req
distinguished_name = req_distinguished_name
[req_distinguished_name]
[ v3_req ]
basicConstraints = CA:FALSE
keyUsage = nonRepudiation, digitalSignature, keyEncipherment
extendedKeyUsage = clientAuth, serverAuth
subjectAltName = DNS:${SERVICE}.${NAMESPACE}.svc
EOF

# Create a certificate authority.
touch ${TMP_DIR}/.rnd
export RANDFILE=${TMP_DIR}/.rnd
openssl genrsa -out ${TMP_DIR}/ca-key.pem 2048
openssl req -x509 -new -nodes -key ${TMP_DIR}/ca-key.pem -days 100000 -out ${TMP_DIR}/ca-cert.pem -subj "/CN=${SERVICE}.${NAMESPACE}.svc"

# Create a server certificate.
openssl genrsa -out ${TMP_DIR}/server-key.pem 2048
# Note the CN is the DNS name of the service of the webhook.
openssl req -new -key ${TMP_DIR}/server-key.pem -out ${TMP_DIR}/server.csr -subj "/CN=${SERVICE}.${NAMESPACE}.svc" -config ${TMP_DIR}/server.conf
openssl x509 -req -in ${TMP_DIR}/server.csr -CA ${TMP_DIR}/ca-cert.pem -CAkey ${TMP_DIR}/ca-key.pem -CAcreateserial -out ${TMP_DIR}/server-cert.pem -days 100000 -extensions v3_req -extfile ${TMP_DIR}/server.conf

# Convert PEM keystore to JKS
keytool -genkey -keyalg RSA -alias tmpalias -dname CN=tmpalias -keystore ${TMP_DIR}/ssl_keystore -storepass "$STOREPASS"
keytool -delete -alias tmpalias -keystore ${TMP_DIR}/ssl_keystore -storepass "$STOREPASS"
openssl pkcs12 -export -in ${TMP_DIR}/server-cert.pem -inkey ${TMP_DIR}/server-key.pem -out ${TMP_DIR}/server-key.p12 -passout "pass:${STOREPASS}"
keytool -importkeystore -srckeystore ${TMP_DIR}/server-key.p12 -srcstoretype PKCS12 -srcstorepass "$STOREPASS" -deststoretype JKS -keystore ${TMP_DIR}/ssl_keystore -storepass "$STOREPASS"

if [[ "$IN_POD" == "true" ]];  then
  TOKEN=$(cat /var/run/secrets/kubernetes.io/serviceaccount/token)

  if [ -n "$OWNER" ]; then
    retries=0
    while [ "$retries" -le 3 ]; do
      retries=$(expr "$retries" + 1)

      echo "Getting UID of \"${OWNER}\" StatefulSet to use in ownerReferences... Attempt number ${retries}."

      STATUS=$(curl -sk \
        -o ${TMP_DIR}/output \
        -w "%{http_code}" \
        -H "Authorization: Bearer $TOKEN" \
        -H 'Accept: application/json' \
        "https://kubernetes.default.svc/apis/apps/v1/namespaces/${NAMESPACE}/statefulsets/${OWNER}")

      if [ "$STATUS" -eq 200 ]; then
        OWNER_UID=$(cat "${TMP_DIR}/output" | jq -r ".metadata.uid")
        echo "UID of \"${OWNER}\" StatefulSet is \"${OWNER_UID}\""
        break
      else
        echo "Failed to get UID of \"${OWNER}\" StatefulSet."
      fi

      sleep 5
    done
  fi

  if [ -n "$OWNER" ] && [ -z "$OWNER_UID" ]; then
    echo "Exiting."
    exit 1
  fi

  if [ -n "${OWNER_UID}" ]; then
    OWNER_META=', "ownerReferences": [{"apiVersion": "apps/v1", "kind": "StatefulSet", "name": "'"$OWNER"'", "uid": "'"$OWNER_UID"'"}]'
  fi

  # Base64 encode secrets and then remove the trailing newline to avoid issues in the curl command
  ca_cert=$(cat ${TMP_DIR}/ca-cert.pem | base64 | tr -d '\n')
  ca_key=$(cat ${TMP_DIR}/ca-key.pem | base64 | tr -d '\n')
  server_cert=$(cat ${TMP_DIR}/server-cert.pem | base64 | tr -d '\n')
  server_key=$(cat ${TMP_DIR}/server-key.pem | base64 | tr -d '\n')
  ssl_keystore=$(cat ${TMP_DIR}/ssl_keystore | base64 | tr -d '\n')
  storepass=$(echo "$STOREPASS" | base64 | tr -d '\n')

  # Create the secret resource
  echo "Creating a secret for the certificate and keys"
  STATUS=$(curl -ik \
    -o ${TMP_DIR}/output \
    -w "%{http_code}" \
    -X POST \
    -H "Authorization: Bearer $TOKEN" \
    -H 'Accept: application/json' \
    -H 'Content-Type: application/json' \
    -d '{
    "kind": "Secret"
    , "apiVersion": "v1"
    , "metadata": {
      "name": "'"$SECRET"'"
      , "namespace": "'"$NAMESPACE"'"
      '"$OWNER_META"'
    }
    , "data": {
      "ca-cert.pem": "'"$ca_cert"'"
      , "ca-key.pem": "'"$ca_key"'"
      , "server-cert.pem": "'"$server_cert"'"
      , "server-key.pem": "'"$server_key"'"
      , "ssl_keystore": "'"$ssl_keystore"'"
      , "storepass": "'"$storepass"'"
    }
  }' \
  https://kubernetes.default.svc/api/v1/namespaces/${NAMESPACE}/secrets)

  cat ${TMP_DIR}/output

  case "$STATUS" in
    201)
      printf "\nSuccess - secret created.\n"
    ;;
    409)
      printf "\nSuccess - secret already exists.\n"
     ;;
     *)
      printf "\nFailed creating secret.\n"
      exit 1
     ;;
  esac
else

  if [ -n "$OWNER" ]; then
    retries=0
    while [ "$retries" -le 3 ]; do
      retries=$(expr "$retries" + 1)

      echo "Getting UID of \"${OWNER}\" StatefulSet to use in ownerReferences... Attempt number ${retries}."

      OWNER_UID=$(kubectl -n "${NAMESPACE}" get statefulset "${OWNER}" -o='jsonpath={.metadata.uid}')

      if [ "$?" -eq 0 ]; then
        echo "UID of \"${OWNER}\" StatefulSet is \"${OWNER_UID}\""
        break
      else
        echo "Failed to get UID of \"${OWNER}\" StatefulSet."
      fi

      sleep 5
    done
  fi

  if [ -n "$OWNER" ] && [ -z "$OWNER_UID" ]; then
    echo "Exiting."
    exit 1
  fi

  kubectl create secret --namespace=${NAMESPACE} generic "$SECRET" \
    --from-file=${TMP_DIR}/ca-key.pem \
    --from-file=${TMP_DIR}/ca-cert.pem \
    --from-file=${TMP_DIR}/server-key.pem \
    --from-file=${TMP_DIR}/server-cert.pem \
    --from-file=${TMP_DIR}/ssl_keystore \
    --from-literal="storepass=${STOREPASS}"

  if [ -n "$OWNER_UID" ]; then
    cat <<EOF | kubectl -n "$NAMESPACE" patch statefulset "$OWNER" -p "$(cat)"
metadata:
    ownerReferences:
      - apiVersion: v1
        kind: StatefulSet
        name: "${OWNER}"
        uid: "${OWNER_UID}"
EOF
  fi
fi

# Clean up after we're done.
printf "\nDeleting ${TMP_DIR}.\n"
rm -rf ${TMP_DIR}
