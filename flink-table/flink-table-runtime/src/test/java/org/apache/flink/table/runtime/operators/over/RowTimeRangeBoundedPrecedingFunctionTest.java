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

package org.apache.flink.table.runtime.operators.over;

import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;

import org.junit.jupiter.api.Test;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link RowTimeRangeBoundedPrecedingFunction}. */
class RowTimeRangeBoundedPrecedingFunctionTest extends RowTimeOverWindowTestBase {

    @Test
    void testStateCleanup() throws Exception {
        RowTimeRangeBoundedPrecedingFunction<RowData> function =
                new RowTimeRangeBoundedPrecedingFunction<>(
                        aggsHandleFunction, accTypes, inputFieldTypes, 2000, 2);
        KeyedProcessOperator<RowData, RowData, RowData> operator =
                new KeyedProcessOperator<>(function);

        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(operator);

        testHarness.open();

        AbstractKeyedStateBackend stateBackend =
                (AbstractKeyedStateBackend) operator.getKeyedStateBackend();

        assertThat(stateBackend.numKeyValueStateEntries())
                .as("Initial state is not empty")
                .isEqualTo(0);

        // put some records
        testHarness.processElement(insertRecord("key", 1L, 100L));
        testHarness.processElement(insertRecord("key", 1L, 100L));
        testHarness.processElement(insertRecord("key", 1L, 500L));

        testHarness.processWatermark(new Watermark(1000L));
        // at this moment we expect the function to have some records in state

        testHarness.processWatermark(new Watermark(4000L));
        // at this moment the function should have cleaned up states

        assertThat(stateBackend.numKeyValueStateEntries())
                .as("State has not been cleaned up")
                .isEqualTo(0);
    }

    @Test
    void testLateRecordMetrics() throws Exception {
        RowTimeRangeBoundedPrecedingFunction<RowData> function =
                new RowTimeRangeBoundedPrecedingFunction<>(
                        aggsHandleFunction, accTypes, inputFieldTypes, 2000, 2);
        KeyedProcessOperator<RowData, RowData, RowData> operator =
                new KeyedProcessOperator<>(function);

        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(operator);

        testHarness.open();

        Counter counter = function.getCounter();

        // put some records
        testHarness.processElement(insertRecord("key", 1L, 100L));
        testHarness.processElement(insertRecord("key", 1L, 100L));
        testHarness.processElement(insertRecord("key", 1L, 500L));

        testHarness.processWatermark(new Watermark(500L));

        // late record
        testHarness.processElement(insertRecord("key", 1L, 400L));

        assertThat(counter.getCount()).isEqualTo(1L);
    }
}
