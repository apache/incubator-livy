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
   * Extract subset data to a new column buffer.
   * @param start start row number
   * @param end end row number, which is not included in the subset data
   */
  public ColumnBuffer extractSubset(int start, int end) {
    ColumnBuffer subset = new ColumnBuffer(type);

    if (start < 0) {
      start = 0;
    }
    if (end > this.currentSize) {
      end = this.currentSize;
    }
    if (end <= start) {
      return subset;
    }

    subset.currentSize = end - start;
    subset.ensureCapacity();
    switch (type) {
      case BOOLEAN:
        System.arraycopy(bools, start, subset.bools, 0, end - start);
        break;
      case BYTE:
        System.arraycopy(bytes, start, subset.bytes, 0, end - start);
        break;
      case SHORT:
        System.arraycopy(shorts, start, subset.shorts, 0, end - start);
        break;
      case INTEGER:
        System.arraycopy(ints, start, subset.ints, 0, end - start);
        break;
      case LONG:
        System.arraycopy(longs, start, subset.longs, 0, end - start);
        break;
      case FLOAT:
        System.arraycopy(floats, start, subset.floats, 0, end - start);
        break;
      case DOUBLE:
        System.arraycopy(doubles, start, subset.doubles, 0, end - start);
        break;
      case BINARY:
        System.arraycopy(buffers, start, subset.buffers, 0, end - start);
        break;
      case STRING:
        System.arraycopy(strings, start, subset.strings, 0, end - start);
        break;
    }
    subset.nulls = getNulls().get(start, end).toByteArray();
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
