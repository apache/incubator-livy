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

function sumWrap(name, val) {
  if (val != null) {
    return "<li><strong>" + name + ": </strong>" + val + "</li>";
  } else {
    return "";
  }
}

function formatError(output) {
  var errStr = output.evalue + "\n";
  var trace = output.traceback;

  for (var x in trace) {
    errStr = errStr + trace[x];
  }

  return preWrap(errStr);
}

function formatDuration(milliseconds) {
  if(milliseconds < 0) {
    return '-'
  }
  if (milliseconds < 100) {
    return milliseconds + " ms";
  }
  var seconds = milliseconds * 1.0 / 1000;
  if (seconds < 1) {
    return seconds.toFixed(1) + " s";
  }
  if (seconds < 60) {
    return seconds.toFixed(0) + " s";
  }
  var minutes = seconds / 60;
  if (minutes < 10) {
    return minutes.toFixed(1) + " min";
  } else if (minutes < 60) {
    return minutes.toFixed(0) + " min";
  }
  var hours = minutes / 60;
  return hours.toFixed(1) + " h";
}

function localDateTime(milliseconds) {
  if(milliseconds <= 0) {
    return '-'
  }
  var now = new Date(milliseconds),
      y = now.getFullYear(),
      m = now.getMonth() + 1,
      d = now.getDate();
  return y + "-" + (m < 10 ? "0" + m : m) + "-" + (d < 10 ? "0" + d : d) + " " + now.toTimeString().substr(0, 8);
}

function statementOutput(output) {
  if (output) {
    var data = output.data;

    if (data && data.hasOwnProperty("text/plain")) {
      return preWrap(data["text/plain"]);
    } else if (output.status == "error") {
      return formatError(output);
    }
  }

  return "";
}

function appendSummary(session) {
  $("#session-summary").append(
    "<h3>Session " + session.id + "</h3>" +
    "<ul class='list-unstyled'>" +
      sumWrap("Application Id", appIdLink(session)) +
      sumWrap("Owner", session.owner) +
      sumWrap("Proxy User", session.proxyUser) +
      sumWrap("Session Kind", session.kind) +
      sumWrap("State", session.state) +
      sumWrap("Logs", logLinks(session, "session")) +
    "</ul>"
  );
}

function loadStatementsTable(statements) {
  $.each(statements, function(index, statement) {
    $("#session-statements .statements-table-body").append(
      "<tr>" +
        tdWrap(statement.id) +
        tdWrap(preWrap(statement.code)) +
        tdWrap(statement.state) +
        tdWrap(progressBar(statement.progress)) +
        tdWrap(statement.output ? statement.output.status : "") +
        tdWrap(statementOutput(statement.output)) +
        tdWrap(localDateTime(statement.started)) +
        tdWrap(localDateTime(statement.completed)) +
        tdWrap(formatDuration(statement.completed - statement.started)) +
       "</tr>"
    );
  });
}

$(document).ready(function () {
  var id = getPathArray().pop();

  $.getJSON(location.origin + prependBasePath("/sessions/") + id, function(response) {
    if (response) {
      appendSummary(response);

      $.getJSON(location.origin + prependBasePath("/sessions/") + id + "/statements", function(statementsRes) {
        if (statementsRes && statementsRes.total_statements > 0) {
          $("#session-statements").load(prependBasePath("/static/html/statements-table.html .statements-template"),
          function() {
            loadStatementsTable(statementsRes.statements);
            $("#statements-table").DataTable();
            $('#session-statements [data-toggle="tooltip"]').tooltip();
          });
        } else {
          $("#session-statements").append('<h4>No statements have been submitted yet.</h4>');
        }
      });
    }
  });
});