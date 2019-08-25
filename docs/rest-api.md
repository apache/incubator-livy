---
layout: page
title: Livy Docs - REST API
tagline: REST API
---
<!--
{% comment %}
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
{% endcomment %}
-->

{% include JB/setup %}

## REST API

### GET /sessions

Returns all the active interactive sessions.


#### Request Parameters

<table class="table">
  <tr><th>Name</th><th>Description</th><th>Type</th></tr>
  <tr>
    <td>from</td>
    <td>The start index to fetch sessions</td>
    <td>int</td>
  </tr>
  <tr>
    <td>size</td>
    <td>Number of sessions to fetch</td>
    <td>int</td>
  </tr>
</table>

#### Response Body

<table class="table">
  <tr><th>Name</th><th>Description</th><th>Type</th></tr>
  <tr>
    <td>from</td>
    <td>The start index to fetch sessions</td>
    <td>int</td>
  </tr>
  <tr>
    <td>total</td>
    <td>Number of sessions to fetch</td>
    <td>int</td>
  </tr>
  <tr>
    <td>sessions</td>
    <td><a href="#session">Session</a> list</td>
    <td>list</td>
  </tr>
</table>

### POST /sessions

Creates a new interactive Scala, Python, or R shell in the cluster.

#### Request Body

<table class="table">
  <tr><th>Name</th><th>Description</th><th>Type</th></tr>
  <tr>
    <td>kind</td>
    <td>The session kind<sup><a href="#footnote1">[1]</a></sup></td>
    <td><a href="#session-kind">session kind</a></td>
  </tr>
  <tr>
    <td>proxyUser</td>
    <td>User to impersonate when starting the session</td>
    <td>string</td>
  </tr>
  <tr>
    <td>jars</td>
    <td>jars to be used in this session</td>
    <td>List of string</td>
  </tr>
  <tr>
    <td>pyFiles</td>
    <td>Python files to be used in this session</td>
    <td>List of string</td>
  </tr>
  <tr>
    <td>files</td>
    <td>files to be used in this session</td>
    <td>List of string</td>
  </tr>
  <tr>
    <td>driverMemory</td>
    <td>Amount of memory to use for the driver process</td>
    <td>string</td>
  </tr>
  <tr>
    <td>driverCores</td>
    <td>Number of cores to use for the driver process</td>
    <td>int</td>
  </tr>
  <tr>
    <td>executorMemory</td>
    <td>Amount of memory to use per executor process</td>
    <td>string</td>
  </tr>
  <tr>
    <td>executorCores</td>
    <td>Number of cores to use for each executor</td>
    <td>int</td>
  </tr>
  <tr>
    <td>numExecutors</td>
    <td>Number of executors to launch for this session</td>
    <td>int</td>
  </tr>
  <tr>
    <td>archives</td>
    <td>Archives to be used in this session</td>
    <td>List of string</td>
  </tr>
  <tr>
    <td>queue</td>
    <td>The name of the YARN queue to which submitted</td>
    <td>string</td>
  </tr>
  <tr>
    <td>name</td>
    <td>The name of this session</td>
    <td>string</td>
  </tr>
  <tr>
    <td>conf</td>
    <td>Spark configuration properties</td>
    <td>Map of key=val</td>
  </tr>
  <tr>
    <td>heartbeatTimeoutInSecond</td>
    <td>Timeout in second to which session be orphaned</td>
    <td>int</td>
  </tr>
</table>

<a id="footnote1">1</a>: Starting with version 0.5.0-incubating this field is not required. To be
compatible with previous versions users can still specify this with spark, pyspark or sparkr,
implying that the submitted code snippet is the corresponding kind.

#### Response Body

The created <a href="#session">Session</a>.


### GET /sessions/{sessionId}

Returns the session information.

#### Response Body

The <a href="#session">Session</a>.


### GET /sessions/{sessionId}/state

Returns the state of session

#### Response

<table class="table">
  <tr><th>Name</th><th>Description</th><th>Type</th></tr>
  <tr>
    <td>id</td>
    <td>Session id</td>
    <td>int</td>
  </tr>
  <tr>
    <td>state</td>
    <td>The current state of session</td>
    <td>string</td>
  </tr>
</table>

### DELETE /sessions/{sessionId}

Kills the <a href="#session">Session</a> job.


### GET /sessions/{sessionId}/log

Gets the log lines from this session.

#### Request Parameters

<table class="table">
  <tr><th>Name</th><th>Description</th><th>Type</th></tr>
  <tr>
    <td>from</td>
    <td>Offset</td>
    <td>int</td>
  </tr>
  <tr>
    <td>size</td>
    <td>Max number of log lines to return</td>
    <td>int</td>
  </tr>
</table>

#### Response Body

<table class="table">
  <tr><th>Name</th><th>Description</th><th>Type</th></tr>
  <tr>
    <td>id</td>
    <td>The session id</td>
    <td>int</td>
  </tr>
  <tr>
    <td>from</td>
    <td>Offset from start of log</td>
    <td>int</td>
  </tr>
  <tr>
    <td>size</td>
    <td>Max number of log lines</td>
    <td>int</td>
  </tr>
  <tr>
    <td>log</td>
    <td>The log lines</td>
    <td>list of strings</td>
  </tr>
</table>

### GET /sessions/{sessionId}/statements

Returns all the statements in a session.

#### Response Body

<table class="table">
  <tr><th>Name</th><th>Description</th><th>Type</th></tr>
  <tr>
    <td>statements</td>
    <td><a href="#statement">statement</a> list</td>
    <td>list</td>
  </tr>
</table>

### POST /sessions/{sessionId}/statements

Runs a statement in a session.

#### Request Body

<table class="table">
  <tr><th>Name</th><th>Description</th><th>Type</th></tr>
  <tr>
    <td>code</td>
    <td>The code to execute</td>
    <td>string</td>
  </tr>
  <tr>
    <td>kind</td>
    <td>The kind of code to execute<sup><a href="#footnote2">[2]</a></sup></td>
    <td><a href="#session-kind">code kind</a></td>
  </tr>
</table>

<a id="footnote2">2</a>: If session kind is not specified or the submitted code is not the kind
specified in session creation, this field should be filled with correct kind.
Otherwise Livy will use kind specified in session creation as the default code kind.

#### Response Body

The <a href="#statement">statement</a> object.


### GET /sessions/{sessionId}/statements/{statementId}

Returns a specified statement in a session.

#### Response Body

The <a href="#statement">statement</a> object.


### POST /sessions/{sessionId}/statements/{statementId}/cancel

Cancel the specified statement in this session.

#### Response Body

<table class="table">
  <tr><th>Name</th><th>Description</th><th>Type</th></tr>
  <tr>
    <td>msg</td>
    <td>is always "canceled"</td>
    <td>string</td>
  </tr>
</table>

### POST /sessions/{sessionId}/completion

Runs a statement in a session.

#### Request Body

<table class="table">
  <tr><th>Name</th><th>Description</th><th>Type</th></tr>
  <tr>
    <td>code</td>
    <td>The code for which completion proposals are requested</td>
    <td>string</td>
  </tr>
  <tr>
    <td>kind</td>
    <td>The kind of code to execute<sup><a href="#footnote2">[2]</a></sup></td>
    <td><a href="#session-kind">code kind</a></td>
  </tr>
  <tr>
    <td>cursor</td>
    <td>cursor position to get proposals</td>
    <td>string</td>
  </tr>
</table>

#### Response Body

<table class="table">
  <tr><th>Name</th><th>Description</th><th>Type</th></tr>
  <tr>
    <td>candidates</td>
    <td>Code completions proposals</td>
    <td>array[string]</td>
  </tr>
</table>

### GET /batches

Returns all the active batch sessions.

#### Request Parameters

<table class="table">
  <tr><th>Name</th><th>Description</th><th>Type</th></tr>
  <tr>
    <td>from</td>
    <td>The start index to fetch sessions</td>
    <td>int</td>
  </tr>
  <tr>
    <td>size</td>
    <td>Number of sessions to fetch</td>
    <td>int</td>
  </tr>
</table>

#### Response Body

<table class="table">
  <tr><th>Name</th><th>Description</th><th>Type</th></tr>
  <tr>
    <td>from</td>
    <td>The start index of fetched sessions</td>
    <td>int</td>
  </tr>
  <tr>
    <td>total</td>
    <td>Number of sessions fetched</td>
    <td>int</td>
  </tr>
  <tr>
    <td>sessions</td>
    <td><a href="#batch">Batch</a> list</td>
    <td>list</td>
  </tr>
</table>

### POST /batches

#### Request Body

<table class="table">
  <tr><th>Name</th><th>Description</th><th>Type</th></tr>
  <tr>
    <td>file</td>
    <td>File containing the application to execute</td>
    <td>path (required)</td>
  </tr>
  <tr>
    <td>proxyUser</td>
    <td>User to impersonate when running the job</td>
    <td>string</td>
  </tr>
  <tr>
    <td>className</td>
    <td>Application Java/Spark main class</td>
    <td>string</td>
  </tr>
  <tr>
    <td>args</td>
    <td>Command line arguments for the application</td>
    <td>list of strings</td>
  </tr>
  <tr>
    <td>jars</td>
    <td>jars to be used in this session</td>
    <td>list of strings</td>
  </tr>
  <tr>
    <td>pyFiles</td>
    <td>Python files to be used in this session</td>
    <td>list of strings</td>
  </tr>
  <tr>
    <td>files</td>
    <td>files to be used in this session</td>
    <td>list of strings</td>
  </tr>
  <tr>
    <td>driverMemory</td>
    <td>Amount of memory to use for the driver process</td>
    <td>string</td>
  </tr>
  <tr>
    <td>driverCores</td>
    <td>Number of cores to use for the driver process</td>
    <td>int</td>
  </tr>
  <tr>
    <td>executorMemory</td>
    <td>Amount of memory to use per executor process</td>
    <td>string</td>
  </tr>
  <tr>
    <td>executorCores</td>
    <td>Number of cores to use for each executor</td>
    <td>int</td>
  </tr>
  <tr>
    <td>numExecutors</td>
    <td>Number of executors to launch for this session</td>
    <td>int</td>
  </tr>
  <tr>
    <td>archives</td>
    <td>Archives to be used in this session</td>
    <td>List of string</td>
  </tr>
  <tr>
    <td>queue</td>
    <td>The name of the YARN queue to which submitted</td>
    <td>string</td>
  </tr>
  <tr>
    <td>name</td>
    <td>The name of this session</td>
    <td>string</td>
  </tr>
  <tr>
    <td>conf</td>
    <td>Spark configuration properties</td>
    <td>Map of key=val</td>
  </tr>
</table>

#### Response Body

The created <a href="#batch">Batch</a> object.


### GET /batches/{batchId}

Returns the batch session information.

#### Response Body

The <a href="#batch">Batch</a>.


### GET /batches/{batchId}/state

Returns the state of batch session

#### Response

<table class="table">
  <tr><th>Name</th><th>Description</th><th>Type</th></tr>
  <tr>
    <td>id</td>
    <td>Batch session id</td>
    <td>int</td>
  </tr>
  <tr>
    <td>state</td>
    <td>The current state of batch session</td>
    <td>string</td>
  </tr>
</table>

### DELETE /batches/{batchId}

Kills the <a href="#batch">Batch</a> job.


### GET /batches/{batchId}/log

Gets the log lines from this batch.

#### Request Parameters

<table class="table">
  <tr><th>Name</th><th>Description</th><th>Type</th></tr>
  <tr>
    <td>from</td>
    <td>Offset</td>
    <td>int</td>
  </tr>
  <tr>
    <td>size</td>
    <td>Max number of log lines to return</td>
    <td>int</td>
  </tr>
</table>

#### Response Body

<table class="table">
  <tr><th>Name</th><th>Description</th><th>Type</th></tr>
  <tr>
    <td>id</td>
    <td>The batch id</td>
    <td>int</td>
  </tr>
  <tr>
    <td>from</td>
    <td>Offset from start of log</td>
    <td>int</td>
  </tr>
  <tr>
    <td>size</td>
    <td>Number of log lines</td>
    <td>int</td>
  </tr>
  <tr>
    <td>log</td>
    <td>The log lines</td>
    <td>list of strings</td>
  </tr>
</table>

## REST Objects

### Session

A session represents an interactive shell.

<table class="table">
  <tr><th>Name</th><th>Description</th><th>Type</th></tr>
  <tr>
    <td>id</td>
    <td>The session id</td>
    <td>int</td>
  </tr>
  <tr>
    <td>appId</td>
    <td>The application id of this session</td>
    <td>string</td>
  </tr>
  <tr>
    <td>owner</td>
    <td>Remote user who submitted this session</td>
    <td>string</td>
  </tr>
  <tr>
    <td>proxyUser</td>
    <td>User to impersonate when running</td>
    <td>string</td>
  </tr>
  <tr>
    <td>kind</td>
    <td>Session kind (spark, pyspark, or sparkr)</td>
    <td><a href="#session-kind">session kind</a></td>
  </tr>
  <tr>
    <td>log</td>
    <td>The log lines</td>
    <td>list of strings</td>
  </tr>
  <tr>
    <td>state</td>
    <td>The session state</td>
    <td>string</td>
  </tr>
  <tr>
    <td>appInfo</td>
    <td>The detailed application info</td>
    <td>Map of key=val</td>
  </tr>
</table>


#### Session State

<table class="table">
  <tr><th>Value</th><th>Description</th></tr>
  <tr>
    <td>not_started</td>
    <td>Session has not been started</td>
  </tr>
  <tr>
    <td>starting</td>
    <td>Session is starting</td>
  </tr>
  <tr>
    <td>idle</td>
    <td>Session is waiting for input</td>
  </tr>
  <tr>
    <td>busy</td>
    <td>Session is executing a statement</td>
  </tr>
  <tr>
    <td>shutting_down</td>
    <td>Session is shutting down</td>
  </tr>
  <tr>
    <td>error</td>
    <td>Session errored out</td>
  </tr>
  <tr>
    <td>dead</td>
    <td>Session has exited</td>
  </tr>
  <tr>
    <td>killed</td>
    <td>Session has been killed</td>
  </tr>
  <tr>
    <td>success</td>
    <td>Session is successfully stopped</td>
  </tr>
</table>

#### Session Kind

<table class="table">
  <tr><th>Value</th><th>Description</th></tr>
  <tr>
    <td>spark</td>
    <td>Interactive Scala Spark session</td>
  </tr>
  <tr>
    <td><a href="#pyspark">pyspark</a></td>
    <td>Interactive Python Spark session</td>
  </tr>
  <tr>
    <td>sparkr</td>
    <td>Interactive R Spark session</td>
  </tr>
  <tr>
    <td>sql</td>
    <td>Interactive SQL Spark session</td>
  </tr>
  </table>

Starting with version 0.5.0-incubating, each session can support all four Scala, Python and R
interpreters with newly added SQL interpreter. The ``kind`` field in session creation 
is no longer required, instead users should specify code kind (spark, pyspark, sparkr or sql) 
during statement submission.

To be compatible with previous versions, users can still specify ``kind`` in session creation,
while ignoring ``kind`` in statement submission. Livy will then use this session
``kind`` as default kind for all the submitted statements.

If users want to submit code other than default ``kind`` specified in session creation, users
need to specify code kind (spark, pyspark, sparkr or sql) during statement submission.

#### pyspark

To change the Python executable the session uses, Livy reads the path from environment variable
``PYSPARK_PYTHON`` (Same as pyspark).

Starting with version 0.5.0-incubating, session kind "pyspark3" is removed, instead users require
to set ``PYSPARK_PYTHON`` to python3 executable.

Like pyspark, if Livy is running in ``local`` mode, just set the environment variable.
If the session is running in ``yarn-cluster`` mode, please set
``spark.yarn.appMasterEnv.PYSPARK_PYTHON`` in SparkConf so the environment variable is passed to
the driver.

### Statement

A statement represents the result of an execution statement.

<table class="table">
  <tr><th>Name</th><th>Description</th><th>Type</th></tr>
  <tr>
    <td>id</td>
    <td>The statement id</td>
    <td>integer</td>
  </tr>
  <tr>
    <td>code</td>
    <td>The execution code</td>
    <td>string</td>
  </tr>
  <tr>
    <td>state</td>
    <td>The execution state</td>
    <td>statement state</td>
  </tr>
  <tr>
    <td>output</td>
    <td>The execution output</td>
    <td>statement output</td>
  </tr>
</table>

#### Statement State

<table class="table">
  <tr><th>Value</th><th>Description</th></tr>
  <tr>
    <td>waiting</td>
    <td>Statement is enqueued but execution hasn't started</td>
  </tr>
  <tr>
    <td>running</td>
    <td>Statement is currently running</td>
  </tr>
  <tr>
    <td>available</td>
    <td>Statement has a response ready</td>
  </tr>
  <tr>
    <td>error</td>
    <td>Statement failed</td>
  </tr>
  <tr>
    <td>cancelling</td>
    <td>Statement is being cancelling</td>
  </tr>
  <tr>
    <td>cancelled</td>
    <td>Statement is cancelled</td>
  </tr>
</table>

#### Statement Output

<table class="table">
  <tr><th>Name</th><th>Description</th><th>Type</th></tr>
  <tr>
    <td>status</td>
    <td>Execution status</td>
    <td>string</td>
  </tr>
  <tr>
    <td>execution_count</td>
    <td>A monotonically increasing number</td>
    <td>integer</td>
  </tr>
  <tr>
    <td>data</td>
    <td>Statement output</td>
    <td>An object mapping a mime type to the result. If the mime type is
    ``application/json``, the value is a JSON value.</td>
  </tr>
</table>

### Batch

<table class="table">
  <tr><th>Name</th><th>Description</th><th>Type</th></tr>
  <tr>
    <td>id</td>
    <td>The session id</td>
    <td>int</td>
  </tr>
  <tr>
    <td>appId</td>
    <td>The application id of this session</td>
    <td>string</td>
  </tr>
  <tr>
    <td>appInfo</td>
    <td>The detailed application info</td>
    <td>Map of key=val</td>
  </tr>
  <tr>
    <td>log</td>
    <td>The log lines</td>
    <td>list of strings</td>
  </tr>
  <tr>
    <td>state</td>
    <td>The batch state</td>
    <td>string</td>
  </tr>
</table>

### Proxy User - `doAs` support
If superuser support is configured, Livy supports the `doAs` query parameter
to specify the user to impersonate. The `doAs` query parameter can be used
on any supported REST endpoint described above to perform the action as the
specified user. If both `doAs` and `proxyUser` are specified during session
or batch creation, the `doAs` parameter takes precedence.
