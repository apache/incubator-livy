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

package org.apache.livy.thriftserver.serde

import org.apache.livy.thriftserver.types.DataType

/**
 * Utility class for (de-)serialize the results from the Spark application and Livy thriftserver.
 */
class ColumnOrientedResultSet(val types: Array[DataType]) {
  val columns: Array[ColumnBuffer] = types.map(new ColumnBuffer(_))
  def addRow(fields: Array[AnyRef]): Unit = {
    var i = 0
    while (i < fields.length) {
      val field = fields(i)
      columns(i).addValue(field)
      i += 1
    }
  }
}
