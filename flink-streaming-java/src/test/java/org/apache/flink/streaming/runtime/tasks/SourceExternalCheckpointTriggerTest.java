/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.core.testutils.MultiShotLatch;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.MasterTriggerRestoreHook;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.checkpoint.ExternallyInducedSource;
import org.apache.flink.streaming.api.functions.source.legacy.ParallelSourceFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.BlockingQueue;

import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * These tests verify the behavior of a source function that triggers checkpoints in response to
 * received events.
 */
@SuppressWarnings("serial")
class SourceExternalCheckpointTriggerTest {

    private static OneShotLatch ready = new OneShotLatch();
    private static MultiShotLatch sync = new MultiShotLatch();

    @BeforeEach
    void resetLatches() {
        ready = new OneShotLatch();
        sync = new MultiShotLatch();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testCheckpointsTriggeredBySource() throws Exception {
        // set up the basic test harness
        final StreamTaskTestHarness<Long> testHarness =
                new StreamTaskTestHarness<>(SourceStreamTask::new, BasicTypeInfo.LONG_TYPE_INFO);

        testHarness.setupOutputForSingletonOperatorChain();
        testHarness.getExecutionConfig().setLatencyTrackingInterval(-1);

        final long numElements = 10;
        final long checkpointEvery = 3;

        // set up the source function
        ExternalCheckpointsSource source =
                new ExternalCheckpointsSource(numElements, checkpointEvery);
        StreamConfig streamConfig = testHarness.getStreamConfig();
        StreamSource<Long, ?> sourceOperator = new StreamSource<>(source);
        streamConfig.setStreamOperator(sourceOperator);
        streamConfig.setOperatorID(new OperatorID());

        // this starts the source thread
        testHarness.invoke();

        final StreamTask<Long, ?> sourceTask = testHarness.getTask();

        ready.await();

        // now send an external trigger that should be ignored
        assertThatFuture(
                        sourceTask.triggerCheckpointAsync(
                                new CheckpointMetaData(32, 829),
                                CheckpointOptions.forCheckpointWithDefaultLocation()))
                .eventuallySucceeds()
                .isEqualTo(true);

        // step by step let the source thread emit elements
        sync.trigger();
        verifyNextElement(testHarness.getOutput(), 1L);
        sync.trigger();
        verifyNextElement(testHarness.getOutput(), 2L);
        sync.trigger();
        verifyNextElement(testHarness.getOutput(), 3L);

        verifyCheckpointBarrier(testHarness.getOutput(), 1L);

        sync.trigger();
        verifyNextElement(testHarness.getOutput(), 4L);

        // now send an regular trigger command that should be ignored
        assertThatFuture(
                        sourceTask.triggerCheckpointAsync(
                                new CheckpointMetaData(34, 900),
                                CheckpointOptions.forCheckpointWithDefaultLocation()))
                .eventuallySucceeds()
                .isEqualTo(true);

        sync.trigger();
        verifyNextElement(testHarness.getOutput(), 5L);
        sync.trigger();
        verifyNextElement(testHarness.getOutput(), 6L);

        verifyCheckpointBarrier(testHarness.getOutput(), 2L);

        // let the remainder run

        for (long l = 7L, checkpoint = 3L; l <= numElements; l++) {
            sync.trigger();
            verifyNextElement(testHarness.getOutput(), l);

            if (l % checkpointEvery == 0) {
                verifyCheckpointBarrier(testHarness.getOutput(), checkpoint++);
            }
        }

        // done!
    }

    @SuppressWarnings("unchecked")
    private void verifyNextElement(BlockingQueue<Object> output, long expectedElement)
            throws InterruptedException {
        Object next = output.take();
        assertThat(next).as("next element is not an event").isInstanceOf(StreamRecord.class);
        assertThat(((StreamRecord<Long>) next).getValue())
                .as("wrong event")
                .isEqualTo(expectedElement);
    }

    private void verifyCheckpointBarrier(BlockingQueue<Object> output, long checkpointId)
            throws InterruptedException {
        Object next = output.take();
        assertThat(next)
                .as("next element is not a checkpoint barrier")
                .isInstanceOf(CheckpointBarrier.class);
        assertThat(((CheckpointBarrier) next).getId())
                .as("wrong checkpoint id")
                .isEqualTo(checkpointId);
    }

    // ------------------------------------------------------------------------

    private static class ExternalCheckpointsSource
            implements ParallelSourceFunction<Long>, ExternallyInducedSource<Long, Object> {

        private final long numEvents;
        private final long checkpointFrequency;

        private CheckpointTrigger trigger;

        ExternalCheckpointsSource(long numEvents, long checkpointFrequency) {
            this.numEvents = numEvents;
            this.checkpointFrequency = checkpointFrequency;
        }

        @Override
        public void run(SourceContext<Long> ctx) throws Exception {
            ready.trigger();

            // for simplicity in this test, we just trigger checkpoints in ascending order
            long checkpoint = 1;

            for (long num = 1; num <= numEvents; num++) {
                sync.await();
                ctx.collect(num);
                if (num % checkpointFrequency == 0) {
                    trigger.triggerCheckpoint(checkpoint++);
                }
            }
        }

        @Override
        public void cancel() {}

        @Override
        public void setCheckpointTrigger(CheckpointTrigger checkpointTrigger) {
            this.trigger = checkpointTrigger;
        }

        @Override
        public MasterTriggerRestoreHook<Object> createMasterTriggerRestoreHook() {
            // not relevant in this test
            throw new UnsupportedOperationException("not implemented");
        }
    }
}
