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

import java.io.Serializable;
import java.util.*;

import scala.collection.JavaConversions;
import scala.runtime.AbstractFunction1;

import org.apache.spark.api.java.JavaRDD;

class PartitionSampleFunction<T> extends AbstractFunction1<scala.collection.Iterator<T>, List<T>>
        implements Serializable {
    private int startIndex;
    private int endIndex;

    PartitionSampleFunction(int startIndex, int endIndex) {
        this.startIndex = startIndex;
        this.endIndex = endIndex;
    }

    @Override
    public List<T> apply(scala.collection.Iterator<T> iterator) {
        List<T> list = new ArrayList<>();
        int index = 0;
        T element = null;
        while (iterator.hasNext()) {
            element = iterator.next();
            if (index >= startIndex && index < endIndex) {
                list.add(element);
            }
            index++;
            if (index > endIndex) {
                break;
            }
        }

        return list;
    }
}

public class RDDStreamIterator<T> implements Iterator<T> {
    private JavaRDD<T> rdd;
    private Integer batchSize;
    private Integer curPartitionIndex;
    private Integer maxPartitionIndex;
    private Integer curRowIndex;
    private List<Integer> partitionSizeList;
    private Iterator<T> iter;

    public RDDStreamIterator(JavaRDD<T> rdd, Integer batchSize) {
        this.rdd = rdd;
        this.batchSize = batchSize;
        this.curPartitionIndex = 0;
        this.maxPartitionIndex = this.rdd.getNumPartitions() - 1;
        this.curRowIndex = 0;
        this.partitionSizeList = this.rdd.mapPartitions(iter -> {
            int count = 0;
            while(iter.hasNext()) {
                iter.next();
                count ++;
            }
            return Collections.singleton(count).iterator();
        }).collect();
        iter = (new ArrayList<T>()).iterator();
    }

    private Iterator<T> collectPartitionByBatch() {
        List<Integer> partitions = Arrays.asList(curPartitionIndex);
        List<T>[] batches = (List<T>[])rdd.context().runJob(rdd.rdd(),
                new PartitionSampleFunction<T>(curRowIndex, curRowIndex + batchSize),
                (scala.collection.Seq) JavaConversions.asScalaBuffer(partitions),
                scala.reflect.ClassTag$.MODULE$.apply(List.class));
        if (batches.length == 0) {
            return (new ArrayList<T>()).iterator();
        }
        return batches[0].iterator();
    }

    public boolean hasNext() {
        if (iter.hasNext()) {
            return true;
        }

        if (curPartitionIndex < maxPartitionIndex) {
            return true;
        }

        if (curPartitionIndex == maxPartitionIndex &&
                curRowIndex < partitionSizeList.get(curPartitionIndex)) {
            return true;
        }

        return false;
    }

    public T next() {
        if (iter.hasNext()) {
            return iter.next();
        }

        if (curPartitionIndex > maxPartitionIndex) {
            return null;
        }

        if (curPartitionIndex == maxPartitionIndex &&
                curRowIndex >= partitionSizeList.get(curPartitionIndex)) {
            return null;
        }

        iter = collectPartitionByBatch();
        if (curRowIndex + batchSize >= partitionSizeList.get(curPartitionIndex)) {
            curPartitionIndex = curPartitionIndex + 1;
            curRowIndex = 0;
        } else {
            curRowIndex = curRowIndex + batchSize;
        }

        return iter.next();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
