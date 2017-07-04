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

package org.apache.livy.client.http;

import java.net.URI;
import java.util.Properties;

import org.apache.livy.LivyClient;
import org.apache.livy.LivyClientFactory;

/**
 * Factory for HTTP Livy clients.
 */
public final class HttpClientFactory implements LivyClientFactory {

  @Override
  public LivyClient createClient(URI uri, Properties config) {
    if (!"http".equals(uri.getScheme()) && !"https".equals(uri.getScheme())) {
      return null;
    }

    return new HttpClient(uri, new HttpConf(config));
  }

}
