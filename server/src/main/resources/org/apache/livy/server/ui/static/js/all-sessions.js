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

function escapeHtml(unescapedText) {
  return $("<div>").text(unescapedText).html()
}

function loadSessionsTable(sessions) {
  $.each(sessions, function(index, session) {
    $("#interactive-sessions .sessions-table-body").append(
      "<tr>" +
        tdWrap(uiLink("session/" + session.id, session.id)) +
        tdWrap(appIdLink(session)) +
        tdWrap(escapeHtml(session.name)) +
        tdWrap(session.owner) +
        tdWrap(session.proxyUser) +
        tdWrap(session.kind) +
        tdWrap(session.state) +
        tdWrap(logLinks(session, "session")) +
        "</tr>"
    );
  });
}

function loadBatchesTable(sessions) {
  $.each(sessions, function(index, session) {
    $("#batches .sessions-table-body").append(
      "<tr>" +
        tdWrap(session.id) +
        tdWrap(appIdLink(session)) +
        tdWrap(escapeHtml(session.name)) +
        tdWrap(session.owner) +
        tdWrap(session.proxyUser) +
        tdWrap(session.state) +
        tdWrap(logLinks(session, "batch")) +
        "</tr>"
    );
  });
}

var numSessions = 0;
var numBatches = 0;

$(document).ready(function () {
  var sessionsReq = $.getJSON(location.origin + prependBasePath("/sessions"), function(response) {
    if (response && response.total > 0) {
      $("#interactive-sessions").load(prependBasePath("/static/html/sessions-table.html .sessions-template"), function() {
        loadSessionsTable(response.sessions);
        $("#interactive-sessions-table").DataTable();
        $('#interactive-sessions [data-toggle="tooltip"]').tooltip();
      });
    }
    numSessions = response.total;
  });

  var batchesReq = $.getJSON(location.origin + prependBasePath("/batches"), function(response) {
    if (response && response.total > 0) {
      $("#batches").load(prependBasePath("/static/html/batches-table.html .sessions-template"), function() {
        loadBatchesTable(response.sessions);
        $("#batches-table").DataTable();
        $('#batches [data-toggle="tooltip"]').tooltip();
      });
    }
    numBatches = response.total;
  });

  $.when(sessionsReq, batchesReq).done(function () {
    if (numSessions + numBatches == 0) {
      $("#all-sessions").append('<h4>No Sessions or Batches have been created yet.</h4>');
    }
  });
});
