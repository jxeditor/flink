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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetricsBuilder;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.checkpoint.SnapshotType;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamConfig.InputConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.StreamMultipleInputProcessorFactory;
import org.apache.flink.streaming.runtime.io.StreamTaskSourceInput;
import org.apache.flink.streaming.runtime.io.checkpointing.CheckpointBarrierHandler;
import org.apache.flink.streaming.runtime.io.checkpointing.CheckpointedInputGate;
import org.apache.flink.streaming.runtime.io.checkpointing.InputProcessorUtil;
import org.apache.flink.streaming.runtime.metrics.MinWatermarkGauge;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.concurrent.FutureUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A {@link StreamTask} for executing a {@link MultipleInputStreamOperator} and supporting the
 * {@link MultipleInputStreamOperator} to select input for reading.
 */
@Internal
public class MultipleInputStreamTask<OUT>
        extends StreamTask<OUT, MultipleInputStreamOperator<OUT>> {
    private static final int MAX_TRACKED_CHECKPOINTS = 100_000;

    private final HashMap<Long, CompletableFuture<Boolean>> pendingCheckpointCompletedFutures =
            new HashMap<>();

    @Nullable private CheckpointBarrierHandler checkpointBarrierHandler;

    public MultipleInputStreamTask(Environment env) throws Exception {
        super(env);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void init() throws Exception {
        StreamConfig configuration = getConfiguration();
        ClassLoader userClassLoader = getUserCodeClassLoader();

        InputConfig[] inputs = configuration.getInputs(userClassLoader);

        WatermarkGauge[] watermarkGauges = new WatermarkGauge[inputs.length];

        for (int i = 0; i < inputs.length; i++) {
            watermarkGauges[i] = new WatermarkGauge();
            mainOperator
                    .getMetricGroup()
                    .gauge(MetricNames.currentInputWatermarkName(i + 1), watermarkGauges[i]);
        }

        MinWatermarkGauge minInputWatermarkGauge = new MinWatermarkGauge(watermarkGauges);
        mainOperator
                .getMetricGroup()
                .gauge(MetricNames.IO_CURRENT_INPUT_WATERMARK, minInputWatermarkGauge);

        List<StreamEdge> inEdges = configuration.getInPhysicalEdges(userClassLoader);

        // Those two number may differ for example when one of the inputs is a union. In that case
        // the number of logical network inputs is smaller compared to the number of inputs (input
        // gates)
        int numberOfNetworkInputs = configuration.getNumberOfNetworkInputs();

        ArrayList[] inputLists = new ArrayList[inputs.length];
        for (int i = 0; i < inputLists.length; i++) {
            inputLists[i] = new ArrayList<>();
        }

        for (int i = 0; i < numberOfNetworkInputs; i++) {
            int inputType = inEdges.get(i).getTypeNumber();
            IndexedInputGate reader = getEnvironment().getInputGate(i);
            inputLists[inputType - 1].add(reader);
        }

        ArrayList<ArrayList<?>> networkInputLists = new ArrayList<>();
        for (ArrayList<?> inputList : inputLists) {
            if (!inputList.isEmpty()) {
                networkInputLists.add(inputList);
            }
        }
        createInputProcessor(
                networkInputLists.toArray(new ArrayList[0]),
                inputs,
                watermarkGauges,
                (index) -> inEdges.get(index).getPartitioner());

        // wrap watermark gauge since registered metrics must be unique
        getEnvironment()
                .getMetricGroup()
                .gauge(MetricNames.IO_CURRENT_INPUT_WATERMARK, minInputWatermarkGauge::getValue);
    }

    protected void createInputProcessor(
            List<IndexedInputGate>[] inputGates,
            InputConfig[] inputs,
            WatermarkGauge[] inputWatermarkGauges,
            Function<Integer, StreamPartitioner<?>> gatePartitioners) {
        checkpointBarrierHandler =
                InputProcessorUtil.createCheckpointBarrierHandler(
                        this,
                        getJobConfiguration(),
                        getConfiguration(),
                        getCheckpointCoordinator(),
                        getTaskNameWithSubtaskAndId(),
                        inputGates,
                        operatorChain.getSourceTaskInputs(),
                        mainMailboxExecutor,
                        timerService);

        CheckpointedInputGate[] checkpointedInputGates =
                InputProcessorUtil.createCheckpointedMultipleInputGate(
                        mainMailboxExecutor,
                        inputGates,
                        getEnvironment().getMetricGroup().getIOMetricGroup(),
                        checkpointBarrierHandler,
                        configuration);

        inputProcessor =
                StreamMultipleInputProcessorFactory.create(
                        this,
                        checkpointedInputGates,
                        inputs,
                        getEnvironment().getIOManager(),
                        getEnvironment().getMemoryManager(),
                        getEnvironment().getMetricGroup().getIOMetricGroup(),
                        setupNumRecordsInCounter(mainOperator),
                        mainOperator,
                        inputWatermarkGauges,
                        getConfiguration(),
                        getEnvironment().getTaskManagerInfo().getConfiguration(),
                        getJobConfiguration(),
                        getExecutionConfig(),
                        getUserCodeClassLoader(),
                        operatorChain,
                        getEnvironment().getTaskStateManager().getInputRescalingDescriptor(),
                        gatePartitioners,
                        getEnvironment().getTaskInfo(),
                        getCanEmitBatchOfRecords());
    }

    protected Optional<CheckpointBarrierHandler> getCheckpointBarrierHandler() {
        return Optional.ofNullable(checkpointBarrierHandler);
    }

    @Override
    public CompletableFuture<Boolean> triggerCheckpointAsync(
            CheckpointMetaData metadata, CheckpointOptions options) {

        if (operatorChain.getSourceTaskInputs().size() == 0) {
            return super.triggerCheckpointAsync(metadata, options);
        }

        // If there are chained sources, we would always only trigger the
        // chained sources for checkpoint. This means that for the checkpoints
        // during the upstream task finished and this task receives the
        // EndOfPartitionEvent, we would not complement barriers for the
        // unfinished network inputs, and the checkpoint would be triggered
        // after received all the EndOfPartitionEvent.
        if (isSynchronous(options.getCheckpointType())) {
            return triggerStopWithSavepointAsync(metadata, options);
        } else {
            return triggerSourcesCheckpointAsync(metadata, options);
        }
    }

    // This is needed for StreamMultipleInputProcessor#processInput to preserve the existing
    // behavior of choosing an input every time a record is emitted. This behavior is good for
    // fairness between input consumption. But it can reduce throughput due to added control
    // flow cost on the per-record code path.
    @Override
    public CanEmitBatchOfRecordsChecker getCanEmitBatchOfRecords() {
        return () -> false;
    }

    private boolean isSynchronous(SnapshotType snapshotType) {
        return snapshotType.isSavepoint() && ((SavepointType) snapshotType).isSynchronous();
    }

    private CompletableFuture<Boolean> triggerSourcesCheckpointAsync(
            CheckpointMetaData metadata, CheckpointOptions options) {
        CompletableFuture<Boolean> resultFuture = new CompletableFuture<>();
        mainMailboxExecutor.execute(
                () -> {
                    try {
                        /*
                         * Contrary to {@link SourceStreamTask}, we are not using here
                         * {@link StreamTask#latestAsyncCheckpointStartDelayNanos} to measure the start delay
                         * metric, but we will be using {@link CheckpointBarrierHandler#getCheckpointStartDelayNanos()}
                         * instead.
                         */
                        pendingCheckpointCompletedFutures.put(
                                metadata.getCheckpointId(), resultFuture);
                        checkPendingCheckpointCompletedFuturesSize();
                        emitBarrierForSources(
                                new CheckpointBarrier(
                                        metadata.getCheckpointId(),
                                        metadata.getTimestamp(),
                                        options));
                    } catch (Exception ex) {
                        // Report the failure both via the Future result but also to the mailbox
                        pendingCheckpointCompletedFutures.remove(metadata.getCheckpointId());
                        resultFuture.completeExceptionally(ex);
                        throw ex;
                    }
                },
                "checkpoint %s with %s",
                metadata,
                options);
        return resultFuture;
    }

    private CompletableFuture<Boolean> triggerStopWithSavepointAsync(
            CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions) {

        CompletableFuture<Void> sourcesStopped = new CompletableFuture<>();
        final StopMode stopMode =
                ((SavepointType) checkpointOptions.getCheckpointType()).shouldDrain()
                        ? StopMode.DRAIN
                        : StopMode.NO_DRAIN;
        mainMailboxExecutor.execute(
                () -> {
                    setSynchronousSavepoint(checkpointMetaData.getCheckpointId());
                    FutureUtils.forward(
                            FutureUtils.waitForAll(
                                    operatorChain.getSourceTaskInputs().stream()
                                            .map(s -> s.getOperator().stop(stopMode))
                                            .collect(Collectors.toList())),
                            sourcesStopped);
                },
                "stop chained Flip-27 source for stop-with-savepoint --drain");

        return sourcesStopped.thenCompose(
                ignore -> triggerSourcesCheckpointAsync(checkpointMetaData, checkpointOptions));
    }

    private void checkPendingCheckpointCompletedFuturesSize() {
        if (pendingCheckpointCompletedFutures.size() > MAX_TRACKED_CHECKPOINTS) {
            ArrayList<Long> pendingCheckpointIds =
                    new ArrayList<>(pendingCheckpointCompletedFutures.keySet());
            pendingCheckpointIds.sort(Long::compareTo);
            for (Long checkpointId :
                    pendingCheckpointIds.subList(
                            0, pendingCheckpointIds.size() - MAX_TRACKED_CHECKPOINTS)) {
                pendingCheckpointCompletedFutures
                        .remove(checkpointId)
                        .completeExceptionally(
                                new IllegalStateException("Too many pending checkpoints"));
            }
        }
    }

    private void emitBarrierForSources(CheckpointBarrier checkpointBarrier) throws IOException {
        for (StreamTaskSourceInput<?> sourceInput : operatorChain.getSourceTaskInputs()) {
            for (InputChannelInfo channelInfo : sourceInput.getChannelInfos()) {
                checkpointBarrierHandler.processBarrier(checkpointBarrier, channelInfo, false);
            }
        }
    }

    @Override
    public void triggerCheckpointOnBarrier(
            CheckpointMetaData checkpointMetaData,
            CheckpointOptions checkpointOptions,
            CheckpointMetricsBuilder checkpointMetrics)
            throws IOException {
        CompletableFuture<Boolean> resultFuture =
                pendingCheckpointCompletedFutures.remove(checkpointMetaData.getCheckpointId());
        try {
            super.triggerCheckpointOnBarrier(
                    checkpointMetaData, checkpointOptions, checkpointMetrics);
            if (resultFuture != null) {
                resultFuture.complete(true);
            }
        } catch (IOException ex) {
            if (resultFuture != null) {
                resultFuture.completeExceptionally(ex);
            }
            throw ex;
        }
    }

    @Override
    public void abortCheckpointOnBarrier(long checkpointId, CheckpointException cause)
            throws IOException {
        CompletableFuture<Boolean> resultFuture =
                pendingCheckpointCompletedFutures.remove(checkpointId);
        if (resultFuture != null) {
            resultFuture.completeExceptionally(cause);
        }
        super.abortCheckpointOnBarrier(checkpointId, cause);
    }

    @Override
    protected void advanceToEndOfEventTime() throws Exception {
        for (Output<StreamRecord<?>> sourceOutput : operatorChain.getChainedSourceOutputs()) {
            sourceOutput.emitWatermark(Watermark.MAX_WATERMARK);
        }
    }
}
