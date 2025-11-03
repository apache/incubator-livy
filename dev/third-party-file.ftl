<#--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

<#function artifactFormat p>
  <#if p.name?index_of('Unnamed') &gt; -1>
    <#return p.artifactId + " (" + p.groupId + ":" + p.artifactId + ":" + p.version + " - " + (p.url!"no url defined") + ")">
  <#else>
    <#return p.name + " (" + p.groupId + ":" + p.artifactId + ":" + p.version + " - " + (p.url!"no url defined") + ")">
 </#if>
</#function>

<#if licenseMap?size == 0>
The project has no dependencies.
<#else>

===============================================================================

APACHE LIVY SUBCOMPONENTS:

The Apache Livy binary distribution includes a number of subcomponents
with separate copyright notices and license terms. Your use of the
code for the these subcomponents is subject to the terms and
conditions of the following licenses.

===============================================================================

We have listed all of these third party libraries and their licenses
below. This file can be regenerated at any time by simply running:

    mvn clean package -DskipTests -Dgenerate-third-party

---------------------------------------------------
Third party Java libraries listed by License type.

PLEASE NOTE: Some dependencies may be listed under multiple licenses if they
are dual-licensed. This is especially true of anything listed as
"GNU General Public Library" below.
---------------------------------------------------
  <#list licenseMap as e>
    <#assign license = e.getKey()/>
    <#assign projects = e.getValue()/>
    <#if projects?size &gt; 0>

  ${license}:

    <#list projects as project>
    * ${artifactFormat(project)}
    </#list>
    </#if>
  </#list>
</#if>
