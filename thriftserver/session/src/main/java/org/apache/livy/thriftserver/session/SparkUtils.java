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

package org.apache.livy.thriftserver.session;

import org.apache.spark.sql.types.*;

/**
 * Utilities that should only be executed on the session side; they'll throw errors on the
 * Livy server side since Spark classes are not available.
 */
final class SparkUtils {

  /**
   * @return the wire types used to encode the given Spark schema.
   */
  public static DataType[] translateSchema(StructType schema) {
    DataType[] types = new DataType[schema.fields().length];
    int idx = 0;
    for (StructField f : schema.fields()) {
      Object ftype = f.dataType();
      if (ftype instanceof BooleanType) {
        types[idx] = DataType.BOOLEAN;
      } else if (ftype instanceof ByteType) {
        types[idx] = DataType.BYTE;
      } else if (ftype instanceof ShortType) {
        types[idx] = DataType.SHORT;
      } else if (ftype instanceof IntegerType) {
        types[idx] = DataType.INTEGER;
      } else if (ftype instanceof LongType) {
        types[idx] = DataType.LONG;
      } else if (ftype instanceof FloatType) {
        types[idx] = DataType.FLOAT;
      } else if (ftype instanceof DoubleType) {
        types[idx] = DataType.DOUBLE;
      } else if (ftype instanceof BinaryType) {
        types[idx] = DataType.BINARY;
      } else {
        types[idx] = DataType.STRING;
      }
      idx++;
    }
    return types;
  }

}
