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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.stream.Stream;

/**
 * Container for the contents of a single column in a result set.
 */
public class ColumnBuffer {

  static final int DEFAULT_SIZE = 100;
  static final String EMPTY_STRING = "";
  static final ByteBuffer EMPTY_BUFFER = ByteBuffer.wrap(new byte[0]);

  private final DataType type;

  /**
   * This is a hack around the fact that Kryo cannot properly serialize an instance of
   * java.util.BitSet, because the data are stored in transient fields. So we manually
   * implement a bit set instead.
   */
  private byte[] nulls;

  private int currentSize;
  private boolean[] bools;
  private byte[] bytes;
  private short[] shorts;
  private int[] ints;
  private long[] longs;
  private float[] floats;
  private double[] doubles;
  private String[] strings;
  private byte[][] buffers;

  public ColumnBuffer() {
    this.type = null;
  }

  public ColumnBuffer(DataType type) {
    this.type = type;

    switch (type) {
    case BOOLEAN:
      bools = new boolean[DEFAULT_SIZE];
      break;
    case BYTE:
      bytes = new byte[DEFAULT_SIZE];
      break;
    case SHORT:
      shorts = new short[DEFAULT_SIZE];
      break;
    case INTEGER:
      ints = new int[DEFAULT_SIZE];
      break;
    case LONG:
      longs = new long[DEFAULT_SIZE];
      break;
    case FLOAT:
      floats = new float[DEFAULT_SIZE];
      break;
    case DOUBLE:
      doubles = new double[DEFAULT_SIZE];
      break;
    case BINARY:
      buffers = new byte[DEFAULT_SIZE][];
      break;
    case STRING:
      strings = new String[DEFAULT_SIZE];
      break;
    }
  }

  private ColumnBuffer(DataType type, byte[] nulls, Object values, int currentSize) {
    this.type = type;
    this.nulls = nulls;
    this.currentSize = currentSize;

    switch (type) {
      case BOOLEAN:
        bools = (boolean[]) values;
        break;
      case BYTE:
        bytes = (byte[]) values;
        break;
      case SHORT:
        shorts = (short[]) values;
        break;
      case INTEGER:
        ints = (int[]) values;
        break;
      case LONG:
        longs = (long[]) values;
        break;
      case FLOAT:
        floats = (float[]) values;
        break;
      case DOUBLE:
        doubles = (double[]) values;
        break;
      case BINARY:
        buffers = (byte[][]) values;
        break;
      case STRING:
        strings = (String[]) values;
        break;
    }
  }

  public DataType getType() {
    return type;
  }

  public Object get(int index) {
    if (index >= currentSize) {
      throw new ArrayIndexOutOfBoundsException(index);
    }

    if (isNull(index)) {
      return null;
    }

    switch (type) {
    case BOOLEAN:
      return bools[index];
    case BYTE:
      return bytes[index];
    case SHORT:
      return shorts[index];
    case INTEGER:
      return ints[index];
    case LONG:
      return longs[index];
    case FLOAT:
      return floats[index];
    case DOUBLE:
      return doubles[index];
    case BINARY:
      return ByteBuffer.wrap(buffers[index]);
    case STRING:
      return strings[index];
    }

    throw new IllegalStateException("ShouldNotReachHere()");
  }

  public int size() {
    return currentSize;
  }

  public void add(Object value) {
    if (value == null) {
      setNull(currentSize);
      currentSize++;
      return;
    }

    ensureCapacity();

    switch (type) {
    case BOOLEAN:
      bools[currentSize] = (boolean) value;
      break;
    case BYTE:
      bytes[currentSize] = (byte) value;
      break;
    case SHORT:
      shorts[currentSize] = (short) value;
      break;
    case INTEGER:
      ints[currentSize] = (int) value;
      break;
    case LONG:
      longs[currentSize] = (long) value;
      break;
    case FLOAT:
      floats[currentSize] = (float) value;
      break;
    case DOUBLE:
      doubles[currentSize] = (double) value;
      break;
    case BINARY:
      buffers[currentSize] = (byte[]) value;
      break;
    case STRING:
      strings[currentSize] = (String) value;
      break;
    }

    currentSize += 1;
  }

  public Object getValues() {
    switch (type) {
    case BOOLEAN:
      return (bools.length != currentSize) ? Arrays.copyOfRange(bools, 0, currentSize) : bools;
    case BYTE:
      return (bytes.length != currentSize) ? Arrays.copyOfRange(bytes, 0, currentSize) : bytes;
    case SHORT:
      return (shorts.length != currentSize) ? Arrays.copyOfRange(shorts, 0, currentSize) : shorts;
    case INTEGER:
      return (ints.length != currentSize) ? Arrays.copyOfRange(ints, 0, currentSize) : ints;
    case LONG:
      return (longs.length != currentSize) ? Arrays.copyOfRange(longs, 0, currentSize) : longs;
    case FLOAT:
      return (floats.length != currentSize) ? Arrays.copyOfRange(floats, 0, currentSize) : floats;
    case DOUBLE:
      return (doubles.length != currentSize) ? Arrays.copyOfRange(doubles, 0, currentSize)
        : doubles;
    case BINARY:
      return toList(Arrays.stream(buffers).map(b -> (b != null) ? ByteBuffer.wrap(b) : null),
          EMPTY_BUFFER);
    case STRING:
      return toList(Arrays.stream(strings), EMPTY_STRING);
    }

    return null;
  }

  public BitSet getNulls() {
    return nulls != null ? BitSet.valueOf(nulls) : new BitSet();
  }

  /**
   * Extract subset data to a new ColumnBuffer. It will remove the extracted data from current
   * ColumnBuffer.
   *
   * @param end index of the end row, exclusive
   */
  public ColumnBuffer extractSubset(int end) {
    if (end > this.currentSize) {
      end = this.currentSize;
    }
    if (end < 0) {
      end = 0;
    }

    byte[] subNulls = getNulls().get(0, end).toByteArray();
    int split = 0;
    ColumnBuffer subset = null;
    switch (type) {
      case BOOLEAN:
        split = Math.min(bools.length, end);
        subset = new ColumnBuffer(type, subNulls, Arrays.copyOfRange(bools, 0, split), end);
        bools = Arrays.copyOfRange(bools, split, bools.length);
        break;
      case BYTE:
        split = Math.min(bytes.length, end);
        subset = new ColumnBuffer(type, subNulls, Arrays.copyOfRange(bytes, 0, split), end);
        bytes = Arrays.copyOfRange(bytes, split, bytes.length);
        break;
      case SHORT:
        split = Math.min(shorts.length, end);
        subset = new ColumnBuffer(type, subNulls, Arrays.copyOfRange(shorts, 0, split), end);
        shorts = Arrays.copyOfRange(shorts, split, shorts.length);
        break;
      case INTEGER:
        split = Math.min(ints.length, end);
        subset = new ColumnBuffer(type, subNulls, Arrays.copyOfRange(ints, 0, split), end);
        ints = Arrays.copyOfRange(ints, split, ints.length);
        break;
      case LONG:
        split = Math.min(longs.length, end);
        subset = new ColumnBuffer(type, subNulls, Arrays.copyOfRange(longs, 0, split), end);
        longs = Arrays.copyOfRange(longs, split, longs.length);
        break;
      case FLOAT:
        split = Math.min(floats.length, end);
        subset = new ColumnBuffer(type, subNulls, Arrays.copyOfRange(floats, 0, split), end);
        floats = Arrays.copyOfRange(floats, split, floats.length);
        break;
      case DOUBLE:
        split = Math.min(doubles.length, end);
        subset = new ColumnBuffer(type, subNulls, Arrays.copyOfRange(doubles, 0, split), end);
        doubles = Arrays.copyOfRange(doubles, split, doubles.length);
        break;
      case BINARY:
        split = Math.min(buffers.length, end);
        subset = new ColumnBuffer(type, subNulls, Arrays.copyOfRange(buffers, 0, split), end);
        buffers = Arrays.copyOfRange(buffers, split, buffers.length);
        break;
      case STRING:
        split = Math.min(strings.length, end);
        subset = new ColumnBuffer(type, subNulls, Arrays.copyOfRange(strings, 0, split), end);
        strings = Arrays.copyOfRange(strings, split, strings.length);
        break;
    }
    nulls = getNulls().get(end, currentSize).toByteArray();
    currentSize = currentSize - end;

    return subset;
  }

  private boolean isNull(int index) {
    if (nulls == null) {
      return false;
    }

    int byteIdx = (index / Byte.SIZE);
    if (byteIdx >= nulls.length) {
      return false;
    }

    int bitIdx = (index % Byte.SIZE);
    return (nulls[byteIdx] & (1 << bitIdx)) != 0;
  }

  /**
   * Transforms and internal buffer into a list that meets Hive expectations. Used for
   * string and binary fields.
   *
   * org.apache.hadoop.hive.serde2.thrift.ColumnBuffer expects a List<String> or List<ByteBuffer>,
   * depending on the column type. The Hive/Thrift stack also dislikes nulls, and returning a list
   * with a different number of elements than expected.
   */
  private <T> List<T> toList(Stream<T> data, T defaultValue) {
    final List<T> ret = new ArrayList<>(currentSize);
    data.limit(currentSize).forEach(e -> {
      ret.add(e != null ? e : defaultValue);
    });
    while (ret.size() < currentSize) {
      ret.add(defaultValue);
    }
    return ret;
  }

  private void setNull(int index) {
    int byteIdx = (index / Byte.SIZE);

    if (nulls == null) {
      nulls = new byte[byteIdx + 1];
    } else if (byteIdx >= nulls.length) {
      nulls = Arrays.copyOf(nulls, byteIdx + 1);
    }

    int bitIdx = (index % Byte.SIZE);
    nulls[byteIdx] = (byte) (nulls[byteIdx] | (1 << bitIdx));
  }

  private void ensureCapacity() {
    int nextSize = (currentSize + DEFAULT_SIZE);
    nextSize = nextSize - (nextSize % DEFAULT_SIZE);

    switch (type) {
    case BOOLEAN:
      if (bools.length <= currentSize) {
        bools = Arrays.copyOf(bools, nextSize);
      }
      break;
    case BYTE:
      if (bytes.length <= currentSize) {
        bytes = Arrays.copyOf(bytes, nextSize);
      }
      break;
    case SHORT:
      if (shorts.length <= currentSize) {
        shorts = Arrays.copyOf(shorts, nextSize);
      }
      break;
    case INTEGER:
      if (ints.length <= currentSize) {
        ints = Arrays.copyOf(ints, nextSize);
      }
      break;
    case LONG:
      if (longs.length <= currentSize) {
        longs = Arrays.copyOf(longs, nextSize);
      }
      break;
    case FLOAT:
      if (floats.length <= currentSize) {
        floats = Arrays.copyOf(floats, nextSize);
      }
      break;
    case DOUBLE:
      if (doubles.length <= currentSize) {
        doubles = Arrays.copyOf(doubles, nextSize);
      }
      break;
    case BINARY:
      if (buffers.length <= currentSize) {
        buffers = Arrays.copyOf(buffers, nextSize);
      }
      break;
    case STRING:
      if (strings.length <= currentSize) {
        strings = Arrays.copyOf(strings, nextSize);
      }
      break;
    }
  }

}
