/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

function getLogPath(type, id) {
  if (type == "session") {
    return "/sessions/" + id + "/log";
  } else if (type == "batch") {
    return "/batches/" + id + "/log";
  } else {
    return "";
  }
}

function parseLog(logLines) {
  // TODO: Separate out stdout, stderr, and YARN Diagnostics into different viewers
  return preWrap(logLines.join("\n"));
}

$(document).ready(function () {
  var pathArr = getPathArray();
  var type = pathArr.shift();
  var id = pathArr.shift();

  $.getJSON(location.origin + getLogPath(type, id), {size: -1}, function(response) {
    if (response) {
      $("#session-log").append(parseLog(response.log));
    }
  });
});