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

// entityMap and escapeHtml are from mustache.js
var entityMap = {
  '&': '&amp;',
  '<': '&lt;',
  '>': '&gt;',
  '"': '&quot;',
  "'": '&#39;',
  '/': '&#x2F;',
  '`': '&#x60;',
  '=': '&#x3D;'
};

var basePath = "";

function escapeHtml(string) {
  return String(string).replace(/[&<>"'`=\/]/g, function fromEntityMap (s) {
    return entityMap[s];
  });
}

function uiLink(relativePath, inner) {
  return anchorLink(prependBasePath("/ui/") + relativePath, inner);
}

function anchorLink(link, inner) {
  return '<a href="' + link + '">' + inner + "</a>";
}

function driverLogLink(session) {
  var driverLogUrl = session.appInfo.driverLogUrl;
  if (driverLogUrl != null) {
    return anchorLink(driverLogUrl, "driver");
  } else {
    return "";
  }
}

function executorsLogLinks(session) {
  var executorLogUrls = session.appInfo.executorLogUrls;
  if (executorLogUrls != null) {
    return executorLogUrls.split(";").map(function (pair) {
      var nameAndLink = pair.split("#");
      return divWrap(anchorLink(nameAndLink[1], nameAndLink[0]));
    }).join("");
  } else {
    return "";
  }
}

function logLinks(session, kind) {
  var sessionLog = divWrap(uiLink(kind + "/" + session.id + "/log", "session"));
  var driverLog =  divWrap(driverLogLink(session));
  var executorsLogs = executorsLogLinks(session);
  return sessionLog + driverLog + executorsLogs;
}

function appIdLink(session) {
  var appUiUrl = session.appInfo.sparkUiUrl;
  if (appUiUrl != null) {
    return anchorLink(appUiUrl, session.appId);
  } else {
    return session.appId;
  }
}

function tdWrap(val) {
  var inner = "";
  if (val != null) {
    inner = val;
  }
  return "<td>" + inner + "</td>";
}

function tdWrapWithClass(val, cl) {
  var inner = "";
  if (val != null) {
    inner = val;
  }
  var clVal = "";
  if (cl != null) {
      clVal = " class=\"" + cl + "\"";
  }
  return "<td" + clVal + ">" + inner + "</td>";
}

function preWrap(inner) {
  return "<pre>" + escapeHtml(inner) + "</pre>";
}

function divWrap(inner) {
  return "<div>" + inner + "</div>";
}

function progressBar(double) {
  var cent = +(double * 100).toFixed(3);
  return '<div class="progress"><div class="progress-bar" style="width:'
    + cent + '%"><span>' + cent + '%</span></div></div>';
}

function getPathArray() {
  var pathArr = location.pathname.split("/");
  var baseUrlEnd = 2 + (basePath.match(/\//g) || []).length;
  pathArr.splice(0, baseUrlEnd);
  return pathArr;
}

function setBasePath(path) {
  basePath = path;
}

function prependBasePath(path) {
  return basePath + path;
}

$.extend( $.fn.dataTable.defaults, {
  stateSave: true,
});
