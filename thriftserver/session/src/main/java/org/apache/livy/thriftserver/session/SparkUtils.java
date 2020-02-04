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

import java.sql.Types;

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

  /**
   * Translates our Thrift data types to Spark types
   */
  public static StructType dataTypesToSchema(DataType[] types) {
    StructField[] fields = new StructField[types.length];
    int idx = 0;
    for (DataType dt : types) {
      org.apache.spark.sql.types.DataType sparkDt = null;
      switch (dt) {
        case BOOLEAN:
          sparkDt = DataTypes.BooleanType;
          break;
        case BYTE:
          sparkDt = DataTypes.ByteType;
          break;
        case SHORT:
          sparkDt = DataTypes.ShortType;
          break;
        case INTEGER:
          sparkDt = DataTypes.IntegerType;
          break;
        case LONG:
          sparkDt = DataTypes.LongType;
          break;
        case FLOAT:
          sparkDt = DataTypes.FloatType;
          break;
        case DOUBLE:
          sparkDt = DataTypes.DoubleType;
          break;
        case BINARY:
          sparkDt = DataTypes.BinaryType;
          break;
        case STRING:
          sparkDt = DataTypes.StringType;
          break;
        default:
          throw new IllegalArgumentException("Invalid data type: " + dt);
      }
      fields[idx] = new StructField(
              "col_" + idx, sparkDt, true, Metadata.empty());
      idx++;
    }
    return new StructType(fields);
  }

  /**
   * This method is ported from Spark Hive Thrift server Type class
   * @param type
   * @return
   */
  public static int toJavaSQLType(org.apache.spark.sql.types.DataType type) {
    if (type instanceof NullType) {
      return Types.NULL;
    } else if (type instanceof BooleanType) {
      return Types.BOOLEAN;
    } else if (type instanceof ByteType) {
      return Types.TINYINT;
    } else if (type instanceof ShortType) {
      return Types.SMALLINT;
    } else if (type instanceof IntegerType) {
      return Types.INTEGER;
    } else if (type instanceof LongType) {
      return Types.BIGINT;
    } else if (type instanceof FloatType) {
      return Types.FLOAT;
    } else if (type instanceof DoubleType) {
      return Types.DOUBLE;
    } else if (type instanceof StringType) {
      return Types.VARCHAR;
    } else if (type instanceof DecimalType) {
      return Types.DECIMAL;
    } else if (type instanceof DateType) {
      return Types.DATE;
    } else if (type instanceof TimestampType) {
      return Types.TIMESTAMP;
    } else if (type instanceof BinaryType) {
      return Types.BINARY;
    } else if (type instanceof ArrayType) {
      return Types.ARRAY;
    } else if (type instanceof MapType) {
      return Types.JAVA_OBJECT;
    } else if (type instanceof StructType) {
      return Types.STRUCT;
    } else {
      return Types.OTHER;
    }
  }

  /**
   * This method is ported from Spark hive Thrift server TypeDescriptor
   * @param type
   * @return
   */
  public static Integer getColumnSize(org.apache.spark.sql.types.DataType type) {
    if (type instanceof ByteType) {
      return 3;
    } else if (type instanceof ShortType) {
      return 5;
    } else if (type instanceof IntegerType) {
      return 10;
    } else if (type instanceof LongType) {
      return 19;
    } else if (type instanceof FloatType) {
      return 7;
    } else if (type instanceof DoubleType) {
      return 15;
    } else if (type instanceof DecimalType) {
      return ((DecimalType)type).precision();
    } else if (type instanceof StringType || type instanceof BinaryType || type instanceof MapType
        || type instanceof ArrayType || type instanceof StructType) {
      return Integer.MAX_VALUE;
    } else if (type instanceof DateType) {
      return 10;
    } else if (type instanceof TimestampType) {
      return 29;
    } else {
      return null;
    }
  }

  /**
   * This method is ported from Spark hive Thrift server TypeDescriptor
   * @param type
   * @return
   */
  public static Integer getDecimalDigits(org.apache.spark.sql.types.DataType type) {
    if (type instanceof BooleanType || type instanceof ByteType || type instanceof ShortType
        || type instanceof IntegerType || type instanceof LongType) {
      return 0;
    } else if (type instanceof FloatType) {
      return 7;
    } else if (type instanceof DoubleType) {
      return 15;
    } else if (type instanceof DecimalType) {
      return ((DecimalType)type).scale();
    } else if (type instanceof TimestampType) {
      return 9;
    } else {
      return null;
    }
  }

  /**
   * This method is ported from Spark Hive Thrift server Type class
   * @param type
   * @return
   */
  public static Integer getNumPrecRadix(org.apache.spark.sql.types.DataType type) {
    if (type instanceof ByteType || type instanceof ShortType || type instanceof IntegerType
        || type instanceof LongType || type instanceof FloatType || type instanceof DoubleType
        || type instanceof DecimalType) {
      return 10;
    } else {
      return null;
    }
  }
}
