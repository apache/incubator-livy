# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Powershell script to start Livy server

Function Create-Dir() 
{ 
  $directoryPath = $args[0]
  $directoryVariable = $args[1]
 
  if (-not (Test-Path $directoryPath))
  {
    New-Item -ItemType Directory -Force -Path $directoryPath
  }

  if (-not (Test-Path $directoryPath))
  { 
    throw [System.ApplicationException] "$env:USERNAME does not have permission to create directory $directoryPath defined by $directoryVariable."
  }
}

Function Rotate-Livy-Log()
{
  $logFilePath = $args[0]
  $maxFileCount = $args[1]

  if (Test-Path $logFilePath) 
  {
    while ($maxFileCount -gt 1)
    {
      $previousFileIndex = $maxFileCount - 1

      if (Test-Path "$logFilePath.$previousFileIndex") { Move-Item "$logFilePath.$previousFileIndex" "$logFilePath.$maxFileCount" -Force }

      $maxFileCount = $previousFileIndex
    }
    
    Move-Item $logFilePath "$logFilePath.$maxFileCount" -Force
  }
}

Function Start-Livy-Server()
{ 
  $livyServerPidDirectory = $args[0]
  $livyServerPidFile = $args[1]
  
  #Check if Livy server is already running

  if (Test-Path $livyServerPidFile) 
  { 
    $livyServerPid = Get-Content $livyServerPidFile

    if ((Get-Process -Id $livyServerPid -ErrorAction SilentlyContinue) -ne $Null) 
    { 
        throw [System.ApplicationException] "Livy server is already running with process Id $livyServerPid. Stop the livy server first."
    } 
  }

  #Check setup and prepare for starting Livy server

  $currentDirectory = Get-Location

  #Preset LIVY_HOME but Will attempt to set LIVY_HOME but this may not be visible to Livy server which requires it.

  if (-not (Test-Path env:LIVY_HOME)) 
  { 
    $LIVY_HOME=(Get-Item $currentDirectory).Parent.FullName
    [Environment]::SetEnvironmentVariable("LIVY_HOME", "$LIVY_HOME", "Process")  
  } 
  else { $LIVY_HOME = $env:LIVY_HOME }

  Write-Host "Using LIVY_HOME as $env:LIVY_HOME."

  if (-not (Test-Path env:LIVY_CONF_DIR)) { $LIVY_CONF_DIR = "$LIVY_HOME\conf" } 
  else { $LIVY_CONF_DIR = $env:LIVY_CONF_DIR }

  if (-not (Test-Path $LIVY_CONF_DIR)) { throw [System.ApplicationException] "Could not find Livy conf directory." }

  if (Test-Path env:JAVA_HOME) { $JAVA_HOME = $env:JAVA_HOME; $javaExecutable = "$JAVA_HOME\bin\java" }
  else { throw [System.ApplicationException] "JAVA_HOME is not set." }

  if (Test-Path env:SPARK_HOME) { $SPARK_HOME = $env:SPARK_HOME }
  else { throw [System.ApplicationException] "SPARK_HOME is not set." }

  if (Test-Path env:HADOOP_HOME) { $HADOOP_HOME = $env:HADOOP_HOME }
  else { throw [System.ApplicationException] "HADOOP_HOME is not set." }

  if (-not (Test-Path env:LIVY_LOG_DIR)) { $LIVY_LOG_DIR = "$LIVY_HOME\logs" }
  else { $LIVY_LOG_DIR = $env:LIVY_LOG_DIR }

  if (-not (Test-Path env:LIVY_MAX_LOG_FILES)) { $LIVY_MAX_LOG_FILES = 5 }
  else { $LIVY_MAX_LOG_FILES = [math]::Max($env:LIVY_MAX_LOG_FILES,  5) }

  Create-Dir $livyServerPidDirectory "LIVY_PID_DIR"
  Create-Dir $LIVY_LOG_DIR "LIVY_LOG_DIR"

  $livyServerLogFilePath = "$LIVY_LOG_DIR\livy-$LIVY_IDENT_STRING-server.out"
  $livyServerErrorFilePath = "$LIVY_LOG_DIR\livy-$LIVY_IDENT_STRING-server.err"

  Rotate-Livy-Log $livyServerLogFilePath $LIVY_MAX_LOG_FILES
  Rotate-Livy-Log $livyServerErrorFilePath $LIVY_MAX_LOG_FILES

  #Start Livy server

  $livyLibDir = "$LIVY_HOME\jars"
  
  if (-not (Test-Path $livyLibDir)) { $livyLibDir = "$LIVY_HOME\server\target\jars" } 
    
  if (-not (Test-Path $livyLibDir)) { throw [System.ApplicationException] "Could not find Livy jars directory under $LIVY_HOME." }

  $livyClassPath = "$livyLibDir\*;$LIVY_CONF_DIR"

  if (-not (Test-Path env:SPARK_CONF_DIR)) { $SPARK_CONF_DIR = "$SPARK_HOME\conf" } 
  else { $SPARK_CONF_DIR = $env:SPARK_CONF_DIR }

  if (-not (Test-Path $SPARK_CONF_DIR)) { throw [System.ApplicationException] "Could not find Spark conf directory." }

  $livyClassPath = "$livyClassPath;$SPARK_CONF_DIR"

  if (-not (Test-Path env:HADOOP_CONF_DIR)) 
  { 
    $HADOOP_CONF_DIR = "$HADOOP_HOME\etc\hadoop\client"

    if (-not (Test-Path $HADOOP_CONF_DIR))
    {
      $HADOOP_CONF_DIR = "$HADOOP_HOME\etc\hadoop"
    }
  } 
  else { $HADOOP_CONF_DIR = $env:HADOOP_CONF_DIR }

  if (-not (Test-Path $HADOOP_CONF_DIR)) {  throw [System.ApplicationException] "Could not find Hadoop conf directory." }

  $livyClassPath = "$livyClassPath;$HADOOP_CONF_DIR"

  if (-not (Test-Path env:HADOOP_YARN_DIR))
  {
    $HADOOP_YARN_DIR = "$HADOOP_HOME\share\hadoop\yarn"
  } 

  if (Test-Path $HADOOP_YARN_DIR)
  {
    $livyClassPath = "$livyClassPath;$HADOOP_YARN_DIR\*"
  }

  if (Test-Path "$LIVY_CONF_DIR\livy-env.cmd" -PathType Leaf) { Invoke-Item "$LIVY_CONF_DIR\livy-env.cmd"}

  $livyStartArguments = "$LIVY_SERVER_JAVA_OPTS -cp `"$livyClassPath;$env:CLASSPATH`" -Dlog4j.configuration=`"file:\$LIVY_CONF_DIR\log4j.properties`" org.apache.livy.server.LivyServer"

  Write-Host "Livy server start command: $javaExecutable $livyStartArguments."

  $livyServerProcess = Start-Process -PassThru $javaExecutable $livyStartArguments -RedirectStandardOutput $livyServerLogFilePath -RedirectStandardError $livyServerErrorFilePath

  $livyServerPid = $livyServerProcess.Id

  Write-Host "Livy server started with process Id $livyServerPid." 
  
  #Write out the process Id of the livy server process
  
  if ((Get-Process -Id $livyServerPid -ErrorAction SilentlyContinue) -ne $Null)
  {
    Set-Content -Path $livyServerPidFile -Value $livyServerPid

    Write-Host "Livy server process Id $livyServerPid recorded at $livyServerPidFile."
  }
  else { throw [System.ApplicationException] "Failed to start Livy server." }
}

Function Stop-Livy-Server()
{
  $livyServerPidFile = $args[0]

  if (Test-Path $livyServerPidFile) 
  { 
    $livyServerPid = Get-Content $livyServerPidFile

    Get-Process -Id $livyServerPid -ErrorAction SilentlyContinue | Stop-Process -PassThru

    Remove-Item $livyServerPidFile
  }
}

Function Query-Livy-Server-Status()
{
  $livyServerPidFile = $args[0]

  if (Test-Path $livyServerPidFile) 
  { 
    $livyServerPid = Get-Content $livyServerPidFile

    if ((Get-Process -Id $livyServerPid -ErrorAction SilentlyContinue) -ne $Null)
    {
      Write-Host "Livy server is running with process Id $livyServerPid."
    }
    else
    {
      Write-Host "Livy server is not running but was started with process Id $livyServerPid."
    }
  }
  else
  {
    Write-Host "Livy server is not running."
  }
}

$scriptUsage = "Usage: livy-server (start|stop|status)"
$scriptAction = $args[0]

Write-Host "Script action: livy-server $scriptAction."

if (-not (Test-Path env:LIVY_IDENT_STRING)) { $LIVY_IDENT_STRING = $env:USERNAME }
else { $LIVY_IDENT_STRING = $env:LIVY_IDENT_STRING }

if (-not (Test-Path env:LIVY_PID_DIR)) { $LIVY_PID_DIR = "$env:USERPROFILE\tmp" }
else { $LIVY_PID_DIR = $env:LIVY_PID_DIR }

$livyPidFile="$LIVY_PID_DIR\livy-$LIVY_IDENT_STRING-server.pid"

switch ($scriptAction)
{
  "start"  { Start-Livy-Server $LIVY_PID_DIR $livyPidFile }
  "stop"   { Stop-Livy-Server $livyPidFile }
  "status" { Query-Livy-Server-Status $livyPidFile } 
  default  { throw [System.ApplicationException] "Unsupported Livy server action" }
}

