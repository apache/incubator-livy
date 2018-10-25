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

function loadThriftSessionsTable(sessions) {
  $.each(sessions, function(index, session) {
    $("#thrift-sessions-table .sessions-table-body").append(
      "<tr>" +
        tdWrap(session.sessionId) +
        tdWrap(uiLink("session/" + session.livySessionId, session.livySessionId)) +
        tdWrap(session.owner) +
        tdWrap(session.createdAt) +
        "</tr>"
    );
  });
}

var numSessions = 0;

$(document).ready(function () {
  var sessionsReq = $.getJSON(location.origin + prependBasePath("/thriftserver/sessions"), function(response) {
    if (response && response.total > 0) {
      $("#thrift-sessions").load(prependBasePath("/static/html/thrift-sessions-table.html .sessions-template"), function() {
        loadThriftSessionsTable(response.sessions);
        $("#thrift-sessions-table").DataTable();
        $('#thrift-sessions [data-toggle="tooltip"]').tooltip();
      });
    }
    numSessions = response.total;
  });

  $.when(sessionsReq).done(function () {
    if (numSessions == 0) {
      $("#thrift-sessions").append('<h4>No open JDBC/ODBC sessions.</h4>');
    }
  });
});
