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

require 'fileutils'
include FileUtils

if not (ENV['SKIP_API'] == '1')
  # Build Scaladoc for Scala API and Javadoc for Java API

  puts "Moving to scala-api module and building Scala API docs."
  cd("../scala-api")

  puts "Running 'mvn scala:doc' from " + pwd + "; this may take a few minutes..."
  system("mvn scala:doc") || raise("Scaladoc maven build failed")

  puts "Moving to api module and building Java API docs."
  cd("../api")

  puts "Running 'mvn javadoc:javadoc -Ppublic-docs' from " + pwd + "; this may take a few minutes..."
  system("mvn javadoc:javadoc -Ppublic-docs") || raise("Javadoc maven build failed")

  puts "Moving back into docs dir."
  cd("../docs")

  puts "Removing old docs"
  puts `rm -rf api`

  # Copy over the ScalaDoc for the Scala API to api/scala.
  # This directory will be copied over to _site when `jekyll` command is run.
  source = "../scala-api/target/site/scaladocs"
  dest = "api/scala"

  puts "Making directory " + dest
  mkdir_p dest

  # From the rubydoc: cp_r('src', 'dest') makes src/dest, but this doesn't.
  puts "cp -r " + source + "/. " + dest
  cp_r(source + "/.", dest)

  # Copy over the JavaDoc for the Java API to api/java.
  source = "../api/target/site/apidocs"
  dest = "api/java"

  puts "Making directory " + dest
  mkdir_p dest

  puts "cp -r " + source + "/. " + dest
  cp_r(source + "/.", dest)

end
