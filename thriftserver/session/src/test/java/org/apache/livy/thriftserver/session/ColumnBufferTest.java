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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.launcher.SparkLauncher;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructField;
import org.junit.Test;
import static org.junit.Assert.*;

public class ColumnBufferTest {

  @Test
  public void testColumnBuffer() throws Exception {
    String warehouse = Files.createTempDirectory("spark-warehouse-").toFile().getAbsolutePath();

    SparkConf conf = new SparkConf()
      .set(SparkLauncher.SPARK_MASTER, "local")
      .set("spark.app.name", getClass().getName())
      .set("spark.sql.warehouse.dir", warehouse);
    SparkContext sc = new SparkContext(conf);

    try {
      SQLContext spark = SQLContext.getOrCreate(sc);

      TestBean tb = new TestBean();
      tb.setId(1);
      tb.setBool(true);
      tb.setBt((byte) 127);
      tb.setSh((short) 42);
      tb.setLg(84);
      tb.setFl(1.23f);
      tb.setDbl(4.567d);
      tb.setDecimal(new BigDecimal("1.23450"));
      tb.setDesc("one");
      tb.setLst(Arrays.asList(1));
      tb.setM(new HashMap<>());
      tb.getM().put("one", 1);
      tb.setBin(new byte[] { 0x08, 0x16, 0x24, 0x32 });
      tb.setChild(new NestedBean());
      tb.getChild().setId(2);
      tb.getChild().setM(new HashMap<>());
      tb.getChild().getM().put("two", 2);

      Encoder<TestBean> encoder = Encoders.bean(TestBean.class);
      Dataset<TestBean> ds = spark.createDataset(Arrays.asList(tb), encoder);

      ds.write().format("parquet").saveAsTable("types_test");

      ResultSet rs = new ResultSet(SparkUtils.translateSchema(ds.schema()), ds.schema().json());
      for (Row r : spark.table("types_test").collectAsList()) {
        Object[] cols = new Object[r.length()];
        for (int i = 0; i < cols.length; i++) {
          cols[i] = r.get(i);
        }
        rs.addRow(cols);
      }

      // The order of columns in the schema is not necessarily the order in which the
      // bean methods are declared. So this is a little more complicated than it should.
      ColumnBuffer[] cols = rs.getColumns();
      int idx = 0;

      for (StructField f : ds.schema().fields()) {
        switch (f.name()) {
        case "id":
          assertEquals(Integer.valueOf(tb.getId()), cols[idx++].get(0));
          break;
        case "bool":
          assertEquals(Boolean.valueOf(tb.getBool()), cols[idx++].get(0));
          break;
        case "bt":
          assertEquals(Byte.valueOf(tb.getBt()), cols[idx++].get(0));
          break;
        case "sh":
          assertEquals(Short.valueOf(tb.getSh()), cols[idx++].get(0));
          break;
        case "lg":
          assertEquals(Long.valueOf(tb.getLg()), cols[idx++].get(0));
          break;
        case "fl":
          assertEquals(Float.valueOf(tb.getFl()), cols[idx++].get(0));
          break;
        case "dbl":
          assertEquals(Double.valueOf(tb.getDbl()), cols[idx++].get(0));
          break;
        case "decimal":
          assertEquals(tb.getDecimal().stripTrailingZeros().toString(), cols[idx++].get(0));
          break;
        case "desc":
          assertEquals(tb.getDesc(), cols[idx++].get(0));
          break;
        case "lst":
          assertEquals("[1]", cols[idx++].get(0));
          break;
        case "m":
          assertEquals("{\"one\":1}", cols[idx++].get(0));
          break;
        case "bin":
          {
            byte[] data = ((ByteBuffer) cols[idx++].get(0)).array();
            assertTrue(Arrays.equals(tb.getBin(), data));
          }
          break;
        case "child":
          // The expected string follows empirical evidence that the Spark bean encoder
          // generates schemas ordered by the field name.
          assertEquals("{\"id\":2,\"m\":{\"two\":2}}", cols[idx++].get(0));
          break;
        default:
          fail("Unexpected schema field: " + f.name());
        }
      }
    } finally {
      sc.stop();
    }
  }

  @Test
  public void testNullsAndResizing() {
    ColumnBuffer col = new ColumnBuffer(DataType.INTEGER);
    for (int i = 0; i < ColumnBuffer.DEFAULT_SIZE * 20; i++) {
      if (i % 13 == 0) {
        col.add(null);
      } else {
        col.add(i);
      }
    }

    int[] values = (int[]) col.getValues();
    BitSet nulls = col.getNulls();
    for (int i = 0; i < ColumnBuffer.DEFAULT_SIZE * 20; i++) {
      if (i % 13 == 0) {
        assertTrue(nulls.get(i));
      } else {
        assertFalse(nulls.get(i));
        assertEquals(i, values[i]);
      }
    }
  }

  @Test
  public void testStringColumn() {
    int nonNullCount = ColumnBuffer.DEFAULT_SIZE * 5;
    int nullCount = ColumnBuffer.DEFAULT_SIZE * 2;

    ColumnBuffer col = new ColumnBuffer(DataType.STRING);
    for (int i = 0; i < nonNullCount; i++) {
      col.add(String.valueOf(i));
    }

    for (int i = 0; i < nullCount; i++) {
      col.add(null);
    }

    @SuppressWarnings("unchecked")
    List<String> values = (List<String>) col.getValues();
    BitSet nulls = col.getNulls();
    assertEquals(nonNullCount + nullCount, values.size());
    for (int i = 0; i < nonNullCount; i++) {
      assertEquals(String.valueOf(i), values.get(i));
      assertFalse(nulls.get(i));
    }
    for (int i = nonNullCount; i < nonNullCount + nullCount; i++) {
      assertEquals(ColumnBuffer.EMPTY_STRING, values.get(i));
      assertTrue(nulls.get(i));
    }
  }

  @Test
  public void testBinaryColumn() {
    int nonNullCount = ColumnBuffer.DEFAULT_SIZE * 5;
    int nullCount = ColumnBuffer.DEFAULT_SIZE * 2;

    ColumnBuffer col = new ColumnBuffer(DataType.BINARY);
    for (int i = 0; i < nonNullCount; i++) {
      byte[] buf = new byte[Integer.SIZE];
      ByteBuffer.wrap(buf).putInt(i);
      col.add(buf);
    }

    for (int i = 0; i < nullCount; i++) {
      col.add(null);
    }

    @SuppressWarnings("unchecked")
    List<ByteBuffer> values = (List<ByteBuffer>) col.getValues();
    BitSet nulls = col.getNulls();
    assertEquals(nonNullCount + nullCount, values.size());
    for (int i = 0; i < nonNullCount; i++) {
      assertEquals(i, values.get(i).getInt());
      assertFalse(nulls.get(i));
    }
    for (int i = nonNullCount; i < nonNullCount + nullCount; i++) {
      assertEquals(ColumnBuffer.EMPTY_BUFFER, values.get(i));
      assertTrue(nulls.get(i));
    }
  }

  @Test
  public void testExtractSubset() {
    testExtractSubsetWithType(DataType.BOOLEAN, new Object[]{true, false, true, false, null});
    testExtractSubsetWithType(DataType.BYTE,
      new Object[]{(byte)0, (byte)1, (byte)2, (byte)3, null});
    testExtractSubsetWithType(DataType.SHORT,
      new Object[]{(short)0, (short)1, (short)2, (short)3, null});
    testExtractSubsetWithType(DataType.INTEGER, new Object[]{1, 2, 3, 4, null});
    testExtractSubsetWithType(DataType.LONG, new Object[]{1L, 2L, 3L, 4L, null});
    testExtractSubsetWithType(DataType.FLOAT, new Object[]{1.0f, 2.0f, 3.0f, 4.0f, null});
    testExtractSubsetWithType(DataType.DOUBLE, new Object[]{1.0, 2.0, 3.0, 4.0, null});
    testExtractSubsetWithType(DataType.BINARY, new Object[]{
      new byte[]{0, 1},
      new byte[]{0},
      new byte[]{},
      new byte[]{0, 1, 2},
      null});
    testExtractSubsetWithType(DataType.STRING, new Object[]{"a", "b", "c", "d", null});

    // When null bits is less than currentSize
    ColumnBuffer buffer = new ColumnBuffer(DataType.STRING);
    buffer.add(null);
    buffer.add("a");
    buffer.add("b");
    ColumnBuffer subset = buffer.extractSubset(2);
    assertEquals(2, subset.size());
    assertNull(subset.get(0));
    assertEquals("a", subset.get(1));
    assertTrue(subset.getNulls().get(0));

    // When value array length is less than currentSize
    buffer = new ColumnBuffer(DataType.STRING);
    for (int i = 0; i < 110; i++) {
      buffer.add(null);
    }
    subset = buffer.extractSubset(110);
    BitSet nullBits = subset.getNulls();
    assertEquals(110, subset.size());
    for (int i = 0; i < 110; i++) {
      assertNull(subset.get(i));
      assertTrue(nullBits.get(i));
    }
    assertEquals(0, buffer.size());
  }

  private void testExtractSubsetWithType(
      DataType type,
      Object[] initValues) {
    ColumnBuffer buffer = new ColumnBuffer(type);

    // Check the passed in test data
    // The number of initial value for test should be 5
    assertEquals(5, initValues.length);
    // The last one should be null
    assertNull(initValues[4]);

    for (int i = 0; i < initValues.length; i++) {
      buffer.add(initValues[i]);
      // Binary column buffer will wrap byte[] in a ByteBuffer
      // We update initValues for the following comparision for Binary ColumnBuffer
      if (type == DataType.BINARY && initValues[i] != null) {
        initValues[i] = ByteBuffer.wrap((byte[]) initValues[i]);
      }
    }

    ColumnBuffer buffer1 = buffer.extractSubset(100);
    assertEquals(5, buffer1.size());
    // null bit should be set
    assertTrue(buffer1.getNulls().get(4));
    assertEquals(initValues[0], buffer1.get(0));
    assertEquals(initValues[1], buffer1.get(1));
    assertEquals(initValues[2], buffer1.get(2));
    assertEquals(initValues[3], buffer1.get(3));
    assertEquals(initValues[4], buffer1.get(4));
    assertEquals(0, buffer.size());

    // extract negative index
    ColumnBuffer buffer2 = buffer1.extractSubset(-1);
    assertEquals(0, buffer2.size());
    assertEquals(5, buffer1.size());

    // Extract single element
    ColumnBuffer buffer3 = buffer1.extractSubset(1);
    assertEquals(1, buffer3.size());
    assertFalse(buffer3.getNulls().get(0));
    assertEquals(initValues[0], buffer3.get(0));
    assertEquals(4, buffer1.size());

    // null bits should not be set
    ColumnBuffer buffer4 = buffer1.extractSubset(2);
    assertEquals(0, buffer4.getNulls().size());
    assertEquals(initValues[1], buffer4.get(0));
    assertEquals(initValues[2], buffer4.get(1));
    assertEquals(2, buffer1.size());
  }

  public static class TestBean {
    private int id;
    private boolean bool;
    private byte bt;
    private short sh;
    private long lg;
    private float fl;
    private double dbl;
    private BigDecimal decimal;
    private String desc;
    private List<Integer> lst;
    private Map<String, Integer> m;
    private byte[] bin;
    private NestedBean child;

    public int getId() { return id; }
    public void setId(int id) { this.id = id; }

    public boolean getBool() { return bool; }
    public void setBool(boolean bool) { this.bool = bool; }

    public byte getBt() { return bt; }
    public void setBt(byte bt) { this.bt = bt; }

    public short getSh() { return sh; }
    public void setSh(short sh) { this.sh = sh; }

    public long getLg() { return lg; }
    public void setLg(long lg) { this.lg = lg; }

    public float getFl() { return fl; }
    public void setFl(float fl) { this.fl = fl; }

    public double getDbl() { return dbl; }
    public void setDbl(double dbl) { this.dbl = dbl; }

    public BigDecimal getDecimal() { return decimal; }
    public void setDecimal(BigDecimal decimal) { this.decimal = decimal; }

    public String getDesc() { return desc; }
    public void setDesc(String desc) { this.desc = desc; }

    public List<Integer> getLst() { return lst; }
    public void setLst(List<Integer> lst) { this.lst = lst; }

    public Map<String, Integer> getM() { return m; }
    public void setM(Map<String, Integer> m) { this.m = m; }

    public byte[] getBin() { return bin; }
    public void setBin(byte[] bin) { this.bin = bin; }

    public NestedBean getChild() { return child; }
    public void setChild(NestedBean child) {
      this.child = child;
    };
  }

  public static class NestedBean {
    private int id;
    private Map<String, Integer> m;

    public int getId() { return id; }
    public void setId(int id) { this.id = id; }

    public Map<String, Integer> getM() { return m; }
    public void setM(Map<String, Integer> m) { this.m = m; }
  }

}
