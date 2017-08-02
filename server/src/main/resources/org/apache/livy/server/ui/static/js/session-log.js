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

function logHeader(name) {
  return "<h4><div data-toggle='tooltip' data-placement='right' "
    + "title='Only the most recent log lines are displayed "
    + "(max log lines is set by the livy.cache-log.size config)'>"
    + name + "</div></h4>";
}

function parseLog(logLines) {
  // display non-standard log formats
  if (!logLines[0].startsWith("stdout")) {
    return preWrap(logLines.join("\n"));
  }

  var stderrIndex = 0;
  var stdoutLines = logLines;
  var stderrLines = [];
  var yarnDiagLines = null;

  for (var i = 1; i < logLines.length; i++) {
    if (stderrIndex == 0 && logLines[i].startsWith("\nstderr")) {
      stdoutLines = logLines.slice(1, i);
      stderrLines = logLines.slice(i, logLines.length);
      stderrIndex = i;
    } else if (logLines[i].startsWith("\nYARN Diagnostics")) {
      stderrLines = logLines.slice(stderrIndex + 1, i);
      yarnDiagLines = logLines.slice(i + 1, logLines.length);
      break;
    }
  }

  var stdoutLog = logHeader("stdout") + preWrap(stdoutLines.join("\n"));
  var stderrLog = logHeader("stderr") + preWrap(stderrLines.join("\n"));
  var yarnDiagLog = "";
  if (yarnDiagLines != null) {
    yarnDiagLog = "<h4>YARN Diagnostics</h4>" + preWrap(yarnDiagLines.join("\n"));
  }

  return stdoutLog + stderrLog + yarnDiagLog;
}

$(document).ready(function () {
  var pathArr = getPathArray();
  var type = pathArr.shift();
  var id = pathArr.shift();

  $.getJSON(location.origin + getLogPath(type, id), {size: -1}, function(response) {
    if (response) {
      $("#session-log").append(parseLog(response.log));
      $('#session-log [data-toggle="tooltip"]').tooltip();
      $("#session-log pre").each(function () {
          $(this).scrollTop($(this)[0].scrollHeight);
      });
    }
  });
});