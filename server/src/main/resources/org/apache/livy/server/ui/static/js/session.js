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
       "</tr>"
    );
  });
}

$(document).ready(function () {
  var id = getPathArray().pop();

  $.getJSON(location.origin + "/sessions/" + id, function(response) {
    if (response) {
      appendSummary(response);

      $.getJSON(location.origin + "/sessions/" + id + "/statements", function(statementsRes) {
        if (statementsRes && statementsRes.total_statements > 0) {
          $("#session-statements").load("/static/html/statements-table.html .statements-template",
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