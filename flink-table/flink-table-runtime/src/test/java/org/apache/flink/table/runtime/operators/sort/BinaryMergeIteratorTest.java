/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.operators.sort;

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.base.IntComparator;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.flink.util.MutableObjectIterator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test of {@link BinaryMergeIterator}. */
class BinaryMergeIteratorTest {

    private RecordComparator comparator;
    private BinaryRowDataSerializer serializer;

    @BeforeEach
    void setup() throws InstantiationException, IllegalAccessException {
        serializer = new BinaryRowDataSerializer(2);
        comparator = IntRecordComparator.INSTANCE;
    }

    private MutableObjectIterator<BinaryRowData> newIterator(
            final int[] keys, final String[] values) {

        BinaryRowData row = serializer.createInstance();
        BinaryRowWriter writer = new BinaryRowWriter(row);
        return new MutableObjectIterator<BinaryRowData>() {

            private int current = 0;

            @Override
            public BinaryRowData next(BinaryRowData reuse) {
                if (current < keys.length) {
                    int key = keys[current];
                    String value = values[current];
                    current++;
                    writer.reset();
                    writer.writeInt(0, key);
                    writer.writeString(1, StringData.fromString(value));
                    writer.complete();
                    return row;
                } else {
                    return null;
                }
            }

            @Override
            public BinaryRowData next() {
                throw new RuntimeException();
            }
        };
    }

    @Test
    void testOneStream() throws Exception {
        List<MutableObjectIterator<BinaryRowData>> iterators = new ArrayList<>();
        iterators.add(
                newIterator(new int[] {1, 2, 4, 5, 10}, new String[] {"1", "2", "4", "5", "10"}));

        final int[] expected = new int[] {1, 2, 4, 5, 10};

        MutableObjectIterator<BinaryRowData> iterator =
                new BinaryMergeIterator<>(
                        iterators,
                        Collections.singletonList(serializer.createInstance()),
                        (o1, o2) -> this.comparator.compare(o1, o2));

        BinaryRowData row = serializer.createInstance();

        int pos = 0;
        while ((row = iterator.next(row)) != null) {
            assertThat(row.getInt(0)).isEqualTo(expected[pos++]);
        }
    }

    @Test
    void testMergeOfTwoStreams() throws Exception {
        List<MutableObjectIterator<BinaryRowData>> iterators = new ArrayList<>();
        iterators.add(
                newIterator(new int[] {1, 2, 4, 5, 10}, new String[] {"1", "2", "4", "5", "10"}));
        iterators.add(
                newIterator(new int[] {3, 6, 7, 10, 12}, new String[] {"3", "6", "7", "10", "12"}));

        final int[] expected = new int[] {1, 2, 3, 4, 5, 6, 7, 10, 10, 12};

        MutableObjectIterator<BinaryRowData> iterator =
                new BinaryMergeIterator<>(
                        iterators, reused(2), (o1, o2) -> this.comparator.compare(o1, o2));

        BinaryRowData row = serializer.createInstance();

        int pos = 0;
        while ((row = iterator.next(row)) != null) {
            assertThat(row.getInt(0)).isEqualTo(expected[pos++]);
        }
    }

    @Test
    void testMergeOfTenStreams() throws Exception {
        List<MutableObjectIterator<BinaryRowData>> iterators = new ArrayList<>();
        iterators.add(
                newIterator(new int[] {1, 2, 17, 23, 23}, new String[] {"A", "B", "C", "D", "E"}));
        iterators.add(
                newIterator(new int[] {2, 6, 7, 8, 9}, new String[] {"A", "B", "C", "D", "E"}));
        iterators.add(
                newIterator(new int[] {4, 10, 11, 11, 12}, new String[] {"A", "B", "C", "D", "E"}));
        iterators.add(
                newIterator(new int[] {3, 6, 7, 10, 12}, new String[] {"A", "B", "C", "D", "E"}));
        iterators.add(
                newIterator(new int[] {7, 10, 15, 19, 44}, new String[] {"A", "B", "C", "D", "E"}));
        iterators.add(
                newIterator(new int[] {6, 6, 11, 17, 18}, new String[] {"A", "B", "C", "D", "E"}));
        iterators.add(
                newIterator(new int[] {1, 2, 4, 5, 10}, new String[] {"A", "B", "C", "D", "E"}));
        iterators.add(
                newIterator(new int[] {5, 10, 19, 23, 29}, new String[] {"A", "B", "C", "D", "E"}));
        iterators.add(
                newIterator(new int[] {9, 9, 9, 9, 9}, new String[] {"A", "B", "C", "D", "E"}));
        iterators.add(
                newIterator(new int[] {8, 8, 14, 14, 15}, new String[] {"A", "B", "C", "D", "E"}));

        TypeComparator<Integer> comparator = new IntComparator(true);

        MutableObjectIterator<BinaryRowData> iterator =
                new BinaryMergeIterator<>(
                        iterators, reused(10), (o1, o2) -> this.comparator.compare(o1, o2));

        BinaryRowData row = serializer.createInstance();

        int pre = 0;
        while ((row = iterator.next(row)) != null) {
            assertThat(comparator.compare(row.getInt(0), pre)).isGreaterThanOrEqualTo(0);
            pre = row.getInt(0);
        }
    }

    private List<BinaryRowData> reused(int size) {
        ArrayList<BinaryRowData> ret = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            ret.add(serializer.createInstance());
        }
        return ret;
    }
}
