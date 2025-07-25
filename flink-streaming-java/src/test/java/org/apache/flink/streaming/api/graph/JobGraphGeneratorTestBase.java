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

package org.apache.flink.streaming.api.graph;

import org.apache.flink.api.common.BatchShuffleMode;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.SupportsConcurrentExecutionAttempts;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.operators.util.UserCodeWrapper;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.SupportsCommitter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.api.connector.source.mocks.MockSource;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializerAdapter;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.InputOutputFormatContainer;
import org.apache.flink.runtime.jobgraph.InputOutputFormatVertex;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessageTypeInfo;
import org.apache.flink.streaming.api.connector.sink2.CommittableSummary;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.connector.sink2.SupportsPostCommitTopology;
import org.apache.flink.streaming.api.connector.sink2.SupportsPreCommitTopology;
import org.apache.flink.streaming.api.connector.sink2.SupportsPreWriteTopology;
import org.apache.flink.streaming.api.datastream.CachedDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSink;
import org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.legacy.InputFormatSourceFunction;
import org.apache.flink.streaming.api.functions.source.legacy.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.apache.flink.streaming.api.legacy.io.TextInputFormat;
import org.apache.flink.streaming.api.legacy.io.TextOutputFormat;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.CoordinatedOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.SourceOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.legacy.YieldingOperatorFactory;
import org.apache.flink.streaming.api.transformations.CacheTransformation;
import org.apache.flink.streaming.api.transformations.MultipleInputTransformation;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.transformations.StreamExchangeMode;
import org.apache.flink.streaming.runtime.partitioner.BroadcastPartitioner;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RebalancePartitioner;
import org.apache.flink.streaming.runtime.partitioner.RescalePartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.SourceOperatorStreamTask;
import org.apache.flink.streaming.util.TestAnyModeReadingStreamOperator;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.Collector;
import org.apache.flink.util.SerializedValue;

import org.apache.flink.shaded.guava33.com.google.common.collect.Iterables;

import org.assertj.core.api.Assertions;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.flink.configuration.CheckpointingOptions.isUnalignedCheckpointEnabled;
import static org.apache.flink.runtime.jobgraph.DistributionPattern.POINTWISE;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.areOperatorsChainable;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

/**
 * Tests for {@link StreamingJobGraphGenerator} and {@link AdaptiveGraphManager}.
 *
 * <p>ATTENTION: This test is extremely brittle. Do NOT remove, add or re-order test cases.
 */
abstract class JobGraphGeneratorTestBase {
    abstract JobGraph createJobGraph(StreamGraph streamGraph);

    @Test
    void testParallelismOneNotChained() {

        // --------- the program ---------

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Tuple2<String, String>> input =
                env.fromData("a", "b", "c", "d", "e", "f")
                        .map(
                                new MapFunction<String, Tuple2<String, String>>() {

                                    @Override
                                    public Tuple2<String, String> map(String value) {
                                        return new Tuple2<>(value, value);
                                    }
                                });

        DataStream<Tuple2<String, String>> result =
                input.keyBy(x -> x.f0)
                        .map(
                                new MapFunction<Tuple2<String, String>, Tuple2<String, String>>() {

                                    @Override
                                    public Tuple2<String, String> map(
                                            Tuple2<String, String> value) {
                                        return value;
                                    }
                                });

        result.addSink(
                new SinkFunction<Tuple2<String, String>>() {

                    @Override
                    public void invoke(Tuple2<String, String> value) {}
                });

        // --------- the job graph ---------

        StreamGraph streamGraph = env.getStreamGraph();
        JobGraph jobGraph = createJobGraph(streamGraph);
        List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();

        assertThat(jobGraph.getNumberOfVertices()).isEqualTo(2);
        assertThat(verticesSorted.get(0).getParallelism()).isEqualTo(1);
        assertThat(verticesSorted.get(1).getParallelism()).isEqualTo(1);

        JobVertex sourceVertex = verticesSorted.get(0);
        JobVertex mapSinkVertex = verticesSorted.get(1);

        assertThat(sourceVertex.getProducedDataSets().get(0).getResultType())
                .isEqualTo(ResultPartitionType.PIPELINED_BOUNDED);
        assertThat(mapSinkVertex.getInputs().get(0).getSource().getResultType())
                .isEqualTo(ResultPartitionType.PIPELINED_BOUNDED);
    }

    /**
     * Tests that disabled checkpointing sets the checkpointing interval to Long.MAX_VALUE and the
     * checkpoint mode to {@link CheckpointingMode#AT_LEAST_ONCE}.
     */
    @Test
    void testDisabledCheckpointing() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromData(0).print();
        StreamGraph streamGraph = env.getStreamGraph();
        assertThat(streamGraph.getCheckpointConfig().isCheckpointingEnabled())
                .withFailMessage("Checkpointing enabled")
                .isFalse();

        JobGraph jobGraph = createJobGraph(streamGraph);

        JobCheckpointingSettings snapshottingSettings = jobGraph.getCheckpointingSettings();
        assertThat(
                        snapshottingSettings
                                .getCheckpointCoordinatorConfiguration()
                                .getCheckpointInterval())
                .isEqualTo(Long.MAX_VALUE);
        assertThat(snapshottingSettings.getCheckpointCoordinatorConfiguration().isExactlyOnce())
                .isFalse();

        assertThat(CheckpointingOptions.getCheckpointingMode(jobGraph.getJobConfiguration()))
                .isEqualTo(CheckpointingMode.AT_LEAST_ONCE);
    }

    @Test
    void testEnabledUnalignedCheckAndDisabledCheckpointing() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getCheckpointConfig().enableUnalignedCheckpoints(true);
        env.fromData(0).print();
        StreamGraph streamGraph = env.getStreamGraph();
        assertThat(streamGraph.getCheckpointConfig().isCheckpointingEnabled())
                .withFailMessage("Checkpointing enabled")
                .isFalse();

        JobGraph jobGraph = createJobGraph(streamGraph);

        assertThat(CheckpointingOptions.getCheckpointingMode(jobGraph.getJobConfiguration()))
                .isEqualTo(CheckpointingMode.AT_LEAST_ONCE);
        assertThat(streamGraph.getCheckpointConfig().isUnalignedCheckpointsEnabled()).isFalse();
        assertThat(isUnalignedCheckpointEnabled(jobGraph.getJobConfiguration())).isFalse();
    }

    @Test
    void testTransformationSetParallelism() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // The default parallelism of the environment (that is inherited by the source)
        // and the parallelism of the map operator needs to be different for this test
        env.setParallelism(4);
        env.fromSequence(1L, 3L).map(i -> i).setParallelism(10).print().setParallelism(20);
        StreamGraph streamGraph = env.getStreamGraph();

        // check the streamGraph parallelism configured
        final List<StreamNode> streamNodes =
                streamGraph.getStreamNodes().stream()
                        .sorted(Comparator.comparingInt(StreamNode::getId))
                        .collect(Collectors.toList());
        assertThat(streamNodes.get(0).isParallelismConfigured()).isFalse();
        assertThat(streamNodes.get(1).isParallelismConfigured()).isTrue();
        assertThat(streamNodes.get(2).isParallelismConfigured()).isTrue();

        // check the jobGraph parallelism configured
        JobGraph jobGraph = createJobGraph(streamGraph);
        List<JobVertex> vertices = jobGraph.getVerticesSortedTopologicallyFromSources();
        assertThat(jobGraph.getNumberOfVertices()).isEqualTo(3);
        assertThat(vertices.get(0).isParallelismConfigured()).isFalse();
        assertThat(vertices.get(1).isParallelismConfigured()).isTrue();
        assertThat(vertices.get(2).isParallelismConfigured()).isTrue();
    }

    @Test
    void testTransformationSetMaxParallelism() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // The max parallelism of the environment (that is inherited by the source)
        // and the parallelism of the map operator needs to be different for this test
        env.setMaxParallelism(4);

        DataStreamSource<Long> source =
                env.fromSequence(1L, 3L); // no explicit max parallelism set, grab from environment.
        SingleOutputStreamOperator<Long> map = source.map(i -> i).setMaxParallelism(10);
        DataStreamSink<Long> sink = map.print().setMaxParallelism(20);

        StreamGraph streamGraph = env.getStreamGraph();

        // check the streamGraph max parallelism is configured correctly
        assertThat(streamGraph.getStreamNode(source.getId()).getMaxParallelism()).isEqualTo(4);
        assertThat(streamGraph.getStreamNode(map.getId()).getMaxParallelism()).isEqualTo(10);
        assertThat(streamGraph.getStreamNode(sink.getTransformation().getId()).getMaxParallelism())
                .isEqualTo(20);
    }

    @Test
    void testChainNodeSetParallelism() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromSequence(1L, 3L).map(value -> value).print().setParallelism(env.getParallelism());
        StreamGraph streamGraph = env.getStreamGraph();

        // check the streamGraph parallelism configured
        final List<StreamNode> streamNodes =
                streamGraph.getStreamNodes().stream()
                        .sorted(Comparator.comparingInt(StreamNode::getId))
                        .collect(Collectors.toList());
        assertThat(streamNodes.get(0).isParallelismConfigured()).isFalse();
        assertThat(streamNodes.get(1).isParallelismConfigured()).isFalse();
        assertThat(streamNodes.get(2).isParallelismConfigured()).isTrue();

        // check the jobGraph parallelism configured
        JobGraph jobGraph = createJobGraph(streamGraph);
        List<JobVertex> vertices = jobGraph.getVerticesSortedTopologicallyFromSources();
        assertThat(jobGraph.getNumberOfVertices()).isEqualTo(1);
        assertThat(vertices.get(0).isParallelismConfigured()).isTrue();
    }

    @Test
    void testChainedSourcesSetParallelism() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        MultipleInputTransformation<Long> transform =
                new MultipleInputTransformation<>(
                        "mit",
                        new UnusedOperatorFactory(),
                        Types.LONG,
                        env.getParallelism(),
                        false);
        DataStreamSource<Long> source1 =
                env.fromSource(
                        new NumberSequenceSource(1, 2),
                        WatermarkStrategy.noWatermarks(),
                        "source1");
        DataStreamSource<Long> source2 =
                env.fromSource(
                        new NumberSequenceSource(1, 2),
                        WatermarkStrategy.noWatermarks(),
                        "source2");
        transform.addInput(source1.getTransformation());
        transform.addInput(source2.getTransformation());
        transform.setChainingStrategy(ChainingStrategy.HEAD_WITH_SOURCES);
        source1.setParallelism(env.getParallelism());
        env.addOperator(transform);

        StreamGraph streamGraph = env.getStreamGraph();

        // check the streamGraph parallelism configured
        final List<StreamNode> streamNodes =
                streamGraph.getStreamNodes().stream()
                        .sorted(Comparator.comparingInt(StreamNode::getId))
                        .collect(Collectors.toList());
        assertThat(streamNodes.get(0).isParallelismConfigured()).isFalse();
        assertThat(streamNodes.get(1).isParallelismConfigured()).isTrue();
        assertThat(streamNodes.get(2).isParallelismConfigured()).isFalse();

        // check the jobGraph parallelism configured
        JobGraph jobGraph = createJobGraph(streamGraph);
        List<JobVertex> vertices = jobGraph.getVerticesSortedTopologicallyFromSources();
        assertThat(jobGraph.getNumberOfVertices()).isEqualTo(1);
        assertThat(vertices.get(0).isParallelismConfigured()).isTrue();
    }

    @Test
    void testDynamicGraphVertexParallelism() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        int defaultParallelism = 20;
        env.setParallelism(defaultParallelism);
        env.fromSequence(1L, 3L).map(value -> value).print();
        StreamGraph streamGraph = env.getStreamGraph();

        for (StreamNode streamNode : streamGraph.getStreamNodes()) {
            assertThat(streamNode.getParallelism()).isEqualTo(defaultParallelism);
        }
        streamGraph.setDynamic(false);
        JobGraph jobGraph = createJobGraph(streamGraph);
        List<JobVertex> vertices = jobGraph.getVerticesSortedTopologicallyFromSources();
        for (JobVertex vertex : vertices) {
            assertThat(vertex.getParallelism()).isEqualTo(defaultParallelism);
        }

        for (StreamNode streamNode : streamGraph.getStreamNodes()) {
            assertThat(streamNode.getParallelism()).isEqualTo(defaultParallelism);
        }
        streamGraph.setDynamic(true);
        jobGraph = createJobGraph(streamGraph);
        vertices = jobGraph.getVerticesSortedTopologicallyFromSources();
        for (JobVertex vertex : vertices) {
            assertThat(vertex.getParallelism()).isEqualTo(ExecutionConfig.PARALLELISM_DEFAULT);
        }
    }

    @Test
    void testUnalignedCheckAndAtLeastOnce() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000, CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().enableUnalignedCheckpoints(true);
        env.fromData(0).print();
        StreamGraph streamGraph = env.getStreamGraph();

        JobGraph jobGraph = createJobGraph(streamGraph);

        assertThat(CheckpointingOptions.getCheckpointingMode(jobGraph.getJobConfiguration()))
                .isEqualTo(CheckpointingMode.AT_LEAST_ONCE);
        assertThat(streamGraph.getCheckpointConfig().isUnalignedCheckpointsEnabled()).isFalse();
        assertThat(isUnalignedCheckpointEnabled(jobGraph.getJobConfiguration())).isFalse();
    }

    @Test
    void generatorForwardsSavepointRestoreSettings() {
        StreamGraph streamGraph =
                new StreamGraph(
                        new Configuration(),
                        new ExecutionConfig(),
                        new CheckpointConfig(),
                        SavepointRestoreSettings.forPath("hello"));

        JobGraph jobGraph = createJobGraph(streamGraph);

        SavepointRestoreSettings savepointRestoreSettings = jobGraph.getSavepointRestoreSettings();
        assertThat(savepointRestoreSettings.getRestorePath()).isEqualTo("hello");
    }

    /** Verifies that the chain start/end is correctly set. */
    @Test
    void testChainStartEndSetting() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // set parallelism to 2 to avoid chaining with source in case when available processors is
        // 1.
        env.setParallelism(2);

        // fromElements -> CHAIN(Map -> Print)
        env.fromData(1, 2, 3)
                .map(
                        new MapFunction<Integer, Integer>() {
                            @Override
                            public Integer map(Integer value) throws Exception {
                                return value;
                            }
                        })
                .print();
        JobGraph jobGraph = createJobGraph(env.getStreamGraph());

        List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();
        JobVertex sourceVertex = verticesSorted.get(0);
        JobVertex mapPrintVertex = verticesSorted.get(1);

        assertThat(sourceVertex.getProducedDataSets().get(0).getResultType())
                .isEqualTo(ResultPartitionType.PIPELINED_BOUNDED);
        assertThat(mapPrintVertex.getInputs().get(0).getSource().getResultType())
                .isEqualTo(ResultPartitionType.PIPELINED_BOUNDED);

        StreamConfig sourceConfig = new StreamConfig(sourceVertex.getConfiguration());
        StreamConfig mapConfig = new StreamConfig(mapPrintVertex.getConfiguration());
        Map<Integer, StreamConfig> chainedConfigs =
                mapConfig.getTransitiveChainedTaskConfigs(getClass().getClassLoader());
        StreamConfig printConfig = chainedConfigs.values().iterator().next();

        assertThat(sourceConfig.isChainStart()).isTrue();
        assertThat(sourceConfig.isChainEnd()).isTrue();

        assertThat(mapConfig.isChainStart()).isTrue();
        assertThat(mapConfig.isChainEnd()).isFalse();

        assertThat(printConfig.isChainStart()).isFalse();
        assertThat(printConfig.isChainEnd()).isTrue();
    }

    @Test
    void testOperatorCoordinatorAddedToJobVertex() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Integer> stream =
                env.fromSource(
                        new MockSource(Boundedness.BOUNDED, 1),
                        WatermarkStrategy.noWatermarks(),
                        "TestingSource");

        OneInputTransformation<Integer, Integer> resultTransform =
                new OneInputTransformation<Integer, Integer>(
                        stream.getTransformation(),
                        "AnyName",
                        new CoordinatedTransformOperatorFactory(),
                        BasicTypeInfo.INT_TYPE_INFO,
                        env.getParallelism());

        new TestingSingleOutputStreamOperator<>(env, resultTransform).print();

        JobGraph jobGraph = createJobGraph(env.getStreamGraph());

        assertThat(jobGraph.getVerticesAsArray()[0].getOperatorCoordinators()).hasSize(2);
    }

    /**
     * Verifies that the resources are merged correctly for chained operators (covers source and
     * sink cases) when generating job graph.
     */
    @Test
    void testResourcesForChainedSourceSink() throws Exception {
        ResourceSpec resource1 = ResourceSpec.newBuilder(0.1, 100).build();
        ResourceSpec resource2 = ResourceSpec.newBuilder(0.2, 200).build();
        ResourceSpec resource3 = ResourceSpec.newBuilder(0.3, 300).build();
        ResourceSpec resource4 = ResourceSpec.newBuilder(0.4, 400).build();
        ResourceSpec resource5 = ResourceSpec.newBuilder(0.5, 500).build();

        Method opMethod = getSetResourcesMethodAndSetAccessible(SingleOutputStreamOperator.class);
        Method sinkMethod = getSetResourcesMethodAndSetAccessible(DataStreamSink.class);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<Integer, Integer>> source =
                env.addSource(
                        new ParallelSourceFunction<Tuple2<Integer, Integer>>() {
                            @Override
                            public void run(SourceContext<Tuple2<Integer, Integer>> ctx)
                                    throws Exception {}

                            @Override
                            public void cancel() {}
                        });
        opMethod.invoke(source, resource1);

        DataStream<Tuple2<Integer, Integer>> map =
                source.map(
                        new MapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
                            @Override
                            public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> value)
                                    throws Exception {
                                return value;
                            }
                        });
        opMethod.invoke(map, resource2);

        // CHAIN(Source -> Map -> Filter)
        DataStream<Tuple2<Integer, Integer>> filter =
                map.filter(
                        new FilterFunction<Tuple2<Integer, Integer>>() {
                            @Override
                            public boolean filter(Tuple2<Integer, Integer> value) throws Exception {
                                return false;
                            }
                        });
        opMethod.invoke(filter, resource3);

        DataStream<Tuple2<Integer, Integer>> reduce =
                filter.keyBy(x -> x.f0)
                        .reduce(
                                new ReduceFunction<Tuple2<Integer, Integer>>() {
                                    @Override
                                    public Tuple2<Integer, Integer> reduce(
                                            Tuple2<Integer, Integer> value1,
                                            Tuple2<Integer, Integer> value2)
                                            throws Exception {
                                        return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
                                    }
                                });
        opMethod.invoke(reduce, resource4);

        DataStreamSink<Tuple2<Integer, Integer>> sink =
                reduce.addSink(
                        new SinkFunction<Tuple2<Integer, Integer>>() {
                            @Override
                            public void invoke(Tuple2<Integer, Integer> value) throws Exception {}
                        });
        sinkMethod.invoke(sink, resource5);

        JobGraph jobGraph = createJobGraph(env.getStreamGraph());

        JobVertex sourceMapFilterVertex =
                jobGraph.getVerticesSortedTopologicallyFromSources().get(0);
        JobVertex reduceSinkVertex = jobGraph.getVerticesSortedTopologicallyFromSources().get(1);

        assertThat(sourceMapFilterVertex.getMinResources())
                .isEqualTo(resource3.merge(resource2).merge(resource1));

        assertThat(reduceSinkVertex.getPreferredResources()).isEqualTo(resource4.merge(resource5));
    }

    @Test
    void testInputOutputFormat(@TempDir java.nio.file.Path outputPath) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> source =
                env.addSource(
                                new InputFormatSourceFunction<>(
                                        new TextInputFormat(new Path("FakePath")),
                                        TypeInformation.of(String.class)))
                        .returns(TypeInformation.of(String.class))
                        .name("source");

        java.nio.file.Path outputFile1 = outputPath.resolve("outputFile1");
        java.nio.file.Path outputFile2 = outputPath.resolve("outputFile2");
        source.writeUsingOutputFormat(new TextOutputFormat<>(new Path(outputFile1.toUri())))
                .name("sink1");
        source.writeUsingOutputFormat(new TextOutputFormat<>(new Path(outputFile2.toUri())))
                .name("sink2");

        StreamGraph streamGraph = env.getStreamGraph();
        JobGraph jobGraph = createJobGraph(streamGraph);
        assertThat(jobGraph.getNumberOfVertices()).isEqualTo(1);

        JobVertex jobVertex = jobGraph.getVertices().iterator().next();
        assertThat(jobVertex).isInstanceOf(InputOutputFormatVertex.class);

        InputOutputFormatContainer formatContainer =
                new InputOutputFormatContainer(
                        new TaskConfig(jobVertex.getConfiguration()),
                        Thread.currentThread().getContextClassLoader());
        Map<OperatorID, UserCodeWrapper<? extends InputFormat<?, ?>>> inputFormats =
                formatContainer.getInputFormats();
        Map<OperatorID, UserCodeWrapper<? extends OutputFormat<?>>> outputFormats =
                formatContainer.getOutputFormats();
        assertThat(inputFormats).hasSize(1);
        assertThat(outputFormats).hasSize(2);

        Map<String, OperatorID> nameToOperatorIds = new HashMap<>();
        StreamConfig headConfig = new StreamConfig(jobVertex.getConfiguration());
        nameToOperatorIds.put(headConfig.getOperatorName(), headConfig.getOperatorID());

        Map<Integer, StreamConfig> chainedConfigs =
                headConfig.getTransitiveChainedTaskConfigs(
                        Thread.currentThread().getContextClassLoader());
        for (StreamConfig config : chainedConfigs.values()) {
            nameToOperatorIds.put(config.getOperatorName(), config.getOperatorID());
        }

        InputFormat<?, ?> sourceFormat =
                inputFormats.get(nameToOperatorIds.get("Source: source")).getUserCodeObject();
        assertThat(sourceFormat).isInstanceOf(TextInputFormat.class);

        OutputFormat<?> sinkFormat1 =
                outputFormats.get(nameToOperatorIds.get("Sink: sink1")).getUserCodeObject();
        assertThat(sinkFormat1).isInstanceOf(TextOutputFormat.class);

        OutputFormat<?> sinkFormat2 =
                outputFormats.get(nameToOperatorIds.get("Sink: sink2")).getUserCodeObject();
        assertThat(sinkFormat2).isInstanceOf(TextOutputFormat.class);
    }

    @Test
    void testCoordinatedOperator() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Integer> source =
                env.fromSource(
                        new MockSource(Boundedness.BOUNDED, 1),
                        WatermarkStrategy.noWatermarks(),
                        "TestSource");
        source.sinkTo(new DiscardingSink<>());

        StreamGraph streamGraph = env.getStreamGraph();
        JobGraph jobGraph = createJobGraph(streamGraph);
        // There should be only one job vertex.
        assertThat(jobGraph.getNumberOfVertices()).isEqualTo(1);

        JobVertex jobVertex = jobGraph.getVerticesAsArray()[0];
        List<SerializedValue<OperatorCoordinator.Provider>> coordinatorProviders =
                jobVertex.getOperatorCoordinators();
        // There should be only one coordinator provider.
        assertThat(coordinatorProviders).hasSize(1);
        // The invokable class should be SourceOperatorStreamTask.
        final ClassLoader classLoader = getClass().getClassLoader();
        assertThat(jobVertex.getInvokableClass(classLoader))
                .isEqualTo(SourceOperatorStreamTask.class);
        StreamOperatorFactory operatorFactory =
                new StreamConfig(jobVertex.getConfiguration())
                        .getStreamOperatorFactory(classLoader);
        assertThat(operatorFactory).isInstanceOf(SourceOperatorFactory.class);
    }

    /** Test setting exchange mode to {@link StreamExchangeMode#PIPELINED}. */
    @Test
    void testExchangeModePipelined() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // fromElements -> Map -> Print
        DataStream<Integer> sourceDataStream = env.fromData(1, 2, 3);

        DataStream<Integer> partitionAfterSourceDataStream =
                new DataStream<>(
                        env,
                        new PartitionTransformation<>(
                                sourceDataStream.getTransformation(),
                                new ForwardPartitioner<>(),
                                StreamExchangeMode.PIPELINED));
        DataStream<Integer> mapDataStream =
                partitionAfterSourceDataStream.map(value -> value).setParallelism(1);

        DataStream<Integer> partitionAfterMapDataStream =
                new DataStream<>(
                        env,
                        new PartitionTransformation<>(
                                mapDataStream.getTransformation(),
                                new RescalePartitioner<>(),
                                StreamExchangeMode.PIPELINED));
        partitionAfterMapDataStream.print().setParallelism(2);

        JobGraph jobGraph = createJobGraph(env.getStreamGraph());

        List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();
        assertThat(verticesSorted).hasSize(2);

        // it can be chained with PIPELINED exchange mode
        JobVertex sourceAndMapVertex = verticesSorted.get(0);

        // PIPELINED exchange mode is translated into PIPELINED_BOUNDED result partition
        assertThat(sourceAndMapVertex.getProducedDataSets().get(0).getResultType())
                .isEqualTo(ResultPartitionType.PIPELINED_BOUNDED);
    }

    /** Test setting exchange mode to {@link StreamExchangeMode#BATCH}. */
    @Test
    void testExchangeModeBatch() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setBufferTimeout(-1);
        // fromElements -> Map -> Print
        DataStream<Integer> sourceDataStream = env.fromData(1, 2, 3);

        DataStream<Integer> partitionAfterSourceDataStream =
                new DataStream<>(
                        env,
                        new PartitionTransformation<>(
                                sourceDataStream.getTransformation(),
                                new ForwardPartitioner<>(),
                                StreamExchangeMode.BATCH));
        DataStream<Integer> mapDataStream =
                partitionAfterSourceDataStream.map(value -> value).setParallelism(1);

        DataStream<Integer> partitionAfterMapDataStream =
                new DataStream<>(
                        env,
                        new PartitionTransformation<>(
                                mapDataStream.getTransformation(),
                                new RescalePartitioner<>(),
                                StreamExchangeMode.BATCH));
        partitionAfterMapDataStream.print().setParallelism(2);

        JobGraph jobGraph = createJobGraph(env.getStreamGraph());

        List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();
        assertThat(verticesSorted).hasSize(3);

        // it can not be chained with BATCH exchange mode
        JobVertex sourceVertex = verticesSorted.get(0);
        JobVertex mapVertex = verticesSorted.get(1);

        // BATCH exchange mode is translated into BLOCKING result partition
        assertThat(sourceVertex.getProducedDataSets().get(0).getResultType())
                .isEqualTo(ResultPartitionType.BLOCKING);
        assertThat(mapVertex.getProducedDataSets().get(0).getResultType())
                .isEqualTo(ResultPartitionType.BLOCKING);
    }

    /** Test setting exchange mode to {@link StreamExchangeMode#UNDEFINED}. */
    @Test
    void testExchangeModeUndefined() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // fromElements -> Map -> Print
        DataStream<Integer> sourceDataStream = env.fromData(1, 2, 3);

        DataStream<Integer> partitionAfterSourceDataStream =
                new DataStream<>(
                        env,
                        new PartitionTransformation<>(
                                sourceDataStream.getTransformation(),
                                new ForwardPartitioner<>(),
                                StreamExchangeMode.UNDEFINED));
        DataStream<Integer> mapDataStream =
                partitionAfterSourceDataStream.map(value -> value).setParallelism(1);

        DataStream<Integer> partitionAfterMapDataStream =
                new DataStream<>(
                        env,
                        new PartitionTransformation<>(
                                mapDataStream.getTransformation(),
                                new RescalePartitioner<>(),
                                StreamExchangeMode.UNDEFINED));
        partitionAfterMapDataStream.print().setParallelism(2);

        JobGraph jobGraph = createJobGraph(env.getStreamGraph());

        List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();
        assertThat(verticesSorted).hasSize(2);

        // it can be chained with UNDEFINED exchange mode
        JobVertex sourceAndMapVertex = verticesSorted.get(0);

        // UNDEFINED exchange mode is translated into PIPELINED_BOUNDED result partition by default
        assertThat(sourceAndMapVertex.getProducedDataSets().get(0).getResultType())
                .isEqualTo(ResultPartitionType.PIPELINED_BOUNDED);
    }

    /** Test setting exchange mode to {@link StreamExchangeMode#HYBRID_FULL}. */
    @Test
    void testExchangeModeHybridFull() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        // fromElements -> Map -> Print
        DataStream<Integer> sourceDataStream = env.fromData(1, 2, 3);

        DataStream<Integer> partitionAfterSourceDataStream =
                new DataStream<>(
                        env,
                        new PartitionTransformation<>(
                                sourceDataStream.getTransformation(),
                                new ForwardPartitioner<>(),
                                StreamExchangeMode.HYBRID_FULL));
        DataStream<Integer> mapDataStream =
                partitionAfterSourceDataStream.map(value -> value).setParallelism(1);

        DataStream<Integer> partitionAfterMapDataStream =
                new DataStream<>(
                        env,
                        new PartitionTransformation<>(
                                mapDataStream.getTransformation(),
                                new RescalePartitioner<>(),
                                StreamExchangeMode.HYBRID_FULL));
        partitionAfterMapDataStream.print().setParallelism(2);

        JobGraph jobGraph = createJobGraph(env.getStreamGraph());

        List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();
        assertThat(verticesSorted).hasSize(2);

        // it can be chained with HYBRID_FULL exchange mode
        JobVertex sourceAndMapVertex = verticesSorted.get(0);

        // HYBRID_FULL exchange mode is translated into HYBRID_FULL result partition
        assertThat(sourceAndMapVertex.getProducedDataSets().get(0).getResultType())
                .isEqualTo(ResultPartitionType.HYBRID_FULL);
    }

    /** Test setting exchange mode to {@link StreamExchangeMode#HYBRID_SELECTIVE}. */
    @Test
    void testExchangeModeHybridSelective() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        // fromElements -> Map -> Print
        DataStream<Integer> sourceDataStream = env.fromData(1, 2, 3);

        DataStream<Integer> partitionAfterSourceDataStream =
                new DataStream<>(
                        env,
                        new PartitionTransformation<>(
                                sourceDataStream.getTransformation(),
                                new ForwardPartitioner<>(),
                                StreamExchangeMode.HYBRID_SELECTIVE));
        DataStream<Integer> mapDataStream =
                partitionAfterSourceDataStream.map(value -> value).setParallelism(1);

        DataStream<Integer> partitionAfterMapDataStream =
                new DataStream<>(
                        env,
                        new PartitionTransformation<>(
                                mapDataStream.getTransformation(),
                                new RescalePartitioner<>(),
                                StreamExchangeMode.HYBRID_SELECTIVE));
        partitionAfterMapDataStream.print().setParallelism(2);

        JobGraph jobGraph = createJobGraph(env.getStreamGraph());

        List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();
        assertThat(verticesSorted).hasSize(2);

        // it can be chained with HYBRID_SELECTIVE exchange mode
        JobVertex sourceAndMapVertex = verticesSorted.get(0);

        // HYBRID_SELECTIVE exchange mode is translated into HYBRID_SELECTIVE result partition
        assertThat(sourceAndMapVertex.getProducedDataSets().get(0).getResultType())
                .isEqualTo(ResultPartitionType.HYBRID_SELECTIVE);
    }

    @Test
    void testStreamingJobTypeByDefault() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromData("test").sinkTo(new DiscardingSink<>());
        JobGraph jobGraph = createJobGraph(env.getStreamGraph());
        assertThat(jobGraph.getJobType()).isEqualTo(JobType.STREAMING);
    }

    @Test
    void testBatchJobType() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.fromData("test").sinkTo(new DiscardingSink<>());
        JobGraph jobGraph = createJobGraph(env.getStreamGraph());
        assertThat(jobGraph.getJobType()).isEqualTo(JobType.BATCH);
    }

    @Test
    void testPartitionTypesInBatchMode() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(4);
        env.disableOperatorChaining();
        DataStream<Integer> source = env.fromData(1);
        source
                // set the same parallelism as the source to make it a FORWARD exchange
                .map(value -> value)
                .setParallelism(1)
                .rescale()
                .map(value -> value)
                .rebalance()
                .map(value -> value)
                .keyBy(value -> value)
                .map(value -> value)
                .sinkTo(new DiscardingSink<>());

        JobGraph jobGraph = createJobGraph(env.getStreamGraph());
        List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();
        assertHasOutputPartitionType(
                verticesSorted.get(0) /* source - forward */, ResultPartitionType.BLOCKING);
        assertHasOutputPartitionType(
                verticesSorted.get(1) /* rescale */, ResultPartitionType.BLOCKING);
        assertHasOutputPartitionType(
                verticesSorted.get(2) /* rebalance */, ResultPartitionType.BLOCKING);
        assertHasOutputPartitionType(
                verticesSorted.get(3) /* keyBy */, ResultPartitionType.BLOCKING);
        assertHasOutputPartitionType(
                verticesSorted.get(4) /* forward - sink */, ResultPartitionType.BLOCKING);
    }

    private void assertHasOutputPartitionType(
            JobVertex jobVertex, ResultPartitionType partitionType) {
        assertThat(jobVertex.getProducedDataSets().get(0).getResultType()).isEqualTo(partitionType);
    }

    @Test
    void testNormalExchangeModeWithBufferTimeout() {
        testCompatibleExchangeModeWithBufferTimeout(StreamExchangeMode.PIPELINED);
    }

    private void testCompatibleExchangeModeWithBufferTimeout(StreamExchangeMode exchangeMode) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setBufferTimeout(100);

        DataStream<Integer> sourceDataStream = env.fromData(1, 2, 3);
        PartitionTransformation<Integer> transformation =
                new PartitionTransformation<>(
                        sourceDataStream.getTransformation(),
                        new RebalancePartitioner<>(),
                        exchangeMode);

        DataStream<Integer> partitionStream = new DataStream<>(env, transformation);
        partitionStream.map(value -> value).print();

        createJobGraph(env.getStreamGraph());
    }

    @Test
    void testDisablingBufferTimeoutWithPipelinedExchanges() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setBufferTimeout(-1);

        env.fromData(1, 2, 3).map(value -> value).print();

        final JobGraph jobGraph = createJobGraph(env.getStreamGraph());
        for (JobVertex vertex : jobGraph.getVertices()) {
            final StreamConfig streamConfig = new StreamConfig(vertex.getConfiguration());
            for (NonChainedOutput output :
                    streamConfig.getVertexNonChainedOutputs(this.getClass().getClassLoader())) {
                assertThat(output.getBufferTimeout()).isEqualTo(-1L);
            }
        }
    }

    /** Test default job type. */
    @Test
    void testDefaultJobType() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamGraph streamGraph =
                new StreamGraphGenerator(
                                Collections.emptyList(), env.getConfig(), env.getCheckpointConfig())
                        .generate();
        JobGraph jobGraph = createJobGraph(streamGraph);
        assertThat(jobGraph.getJobType()).isEqualTo(JobType.STREAMING);
    }

    @Test
    void testYieldingOperatorNotChainableToTaskChainedToLegacySource() {
        // TODO: this test can be removed when the legacy SourceFunction API gets removed
        StreamExecutionEnvironment chainEnv = StreamExecutionEnvironment.createLocalEnvironment(1);

        chainEnv.addSource(new LegacySource())
                .map((x) -> x)
                // not chainable because of YieldingOperatorFactory and legacy source
                .transform(
                        "test", BasicTypeInfo.INT_TYPE_INFO, new YieldingTestOperatorFactory<>());

        final StreamGraph streamGraph = chainEnv.getStreamGraph();

        final List<StreamNode> streamNodes =
                streamGraph.getStreamNodes().stream()
                        .sorted(Comparator.comparingInt(StreamNode::getId))
                        .collect(Collectors.toList());
        assertThat(
                        areOperatorsChainable(
                                streamNodes.get(0), streamNodes.get(1), streamGraph, false))
                .isTrue();
        assertThat(
                        areOperatorsChainable(
                                streamNodes.get(1), streamNodes.get(2), streamGraph, false))
                .isFalse();
    }

    @Test
    void testJobGraphGenerationWithManyYieldingOperatorsDoesNotHang() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);

        SingleOutputStreamOperator<Long> operator =
                env.fromSource(
                                new NumberSequenceSource(0, 10),
                                WatermarkStrategy.noWatermarks(),
                                "input")
                        .map((x) -> x);

        // add 40 YieldingOperatorFactory
        for (int i = 0; i < 40; i++) {
            operator =
                    operator.transform(
                            "test",
                            BasicTypeInfo.LONG_TYPE_INFO,
                            new YieldingTestOperatorFactory<>());
        }

        operator.sinkTo(new DiscardingSink<>());

        assertThat(CompletableFuture.runAsync(() -> createJobGraph(env.getStreamGraph())))
                .succeedsWithin(Duration.ofMinutes(1));
    }

    @Test
    void testYieldingOperatorChainableToTaskNotChainedToLegacySource() {
        StreamExecutionEnvironment chainEnv = StreamExecutionEnvironment.createLocalEnvironment(1);

        chainEnv.fromData(1)
                .disableChaining()
                .map((x) -> x)
                .transform(
                        "test", BasicTypeInfo.INT_TYPE_INFO, new YieldingTestOperatorFactory<>());

        final StreamGraph streamGraph = chainEnv.getStreamGraph();

        final List<StreamNode> streamNodes =
                streamGraph.getStreamNodes().stream()
                        .sorted(Comparator.comparingInt(StreamNode::getId))
                        .collect(Collectors.toList());
        assertThat(
                        areOperatorsChainable(
                                streamNodes.get(0), streamNodes.get(1), streamGraph, false))
                .isFalse();
        assertThat(
                        areOperatorsChainable(
                                streamNodes.get(1), streamNodes.get(2), streamGraph, false))
                .isTrue();
    }

    /**
     * Tests that {@link YieldingOperatorFactory} are not chained to legacy sources, see
     * FLINK-16219.
     */
    @Test
    void testYieldingOperatorProperlyChainedOnLegacySources() {
        // TODO: this test can be removed when the legacy SourceFunction API gets removed
        StreamExecutionEnvironment chainEnv = StreamExecutionEnvironment.createLocalEnvironment(1);

        chainEnv.addSource(new LegacySource())
                .map((x) -> x)
                // should automatically break chain here
                .transform("test", BasicTypeInfo.INT_TYPE_INFO, new YieldingTestOperatorFactory<>())
                .map((x) -> x)
                .transform("test", BasicTypeInfo.INT_TYPE_INFO, new YieldingTestOperatorFactory<>())
                .map((x) -> x)
                .sinkTo(new DiscardingSink<>());

        final JobGraph jobGraph = createJobGraph(chainEnv.getStreamGraph());

        final List<JobVertex> vertices = jobGraph.getVerticesSortedTopologicallyFromSources();
        assertThat(vertices).hasSize(2);
        assertThat(vertices.get(0).getOperatorIDs()).hasSize(2);
        assertThat(vertices.get(1).getOperatorIDs()).hasSize(5);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testChainingOfOperatorsWithDifferentMaxParallelism(
            boolean chainingOfOperatorsWithDifferentMaxParallelismEnabled) throws Exception {
        final Configuration configuration = new Configuration();
        configuration.set(
                PipelineOptions.OPERATOR_CHAINING_CHAIN_OPERATORS_WITH_DIFFERENT_MAX_PARALLELISM,
                chainingOfOperatorsWithDifferentMaxParallelismEnabled);
        configuration.set(PipelineOptions.MAX_PARALLELISM, 10);
        try (StreamExecutionEnvironment chainEnv =
                StreamExecutionEnvironment.createLocalEnvironment(1, configuration)) {
            chainEnv.fromData(1)
                    .map(x -> x)
                    // should automatically break chain here
                    .map(x -> x)
                    .setMaxParallelism(1)
                    .map(x -> x);

            final JobGraph jobGraph = createJobGraph(chainEnv.getStreamGraph());

            final List<JobVertex> vertices = jobGraph.getVerticesSortedTopologicallyFromSources();
            if (chainingOfOperatorsWithDifferentMaxParallelismEnabled) {
                assertThat(vertices).hasSize(1);
                assertThat(vertices.get(0).getOperatorIDs()).hasSize(4);
            } else {
                assertThat(vertices).hasSize(3);
                assertThat(vertices.get(0).getOperatorIDs()).hasSize(2);
                assertThat(vertices.get(1).getOperatorIDs()).hasSize(1);
                assertThat(vertices.get(1).getOperatorIDs()).hasSize(1);
            }
        }
    }

    /** Tests that {@link YieldingOperatorFactory} are chained to new sources, see FLINK-20444. */
    @Test
    void testYieldingOperatorProperlyChainedOnNewSources() {
        StreamExecutionEnvironment chainEnv = StreamExecutionEnvironment.createLocalEnvironment(1);

        chainEnv.fromSource(
                        new NumberSequenceSource(0, 10), WatermarkStrategy.noWatermarks(), "input")
                .map((x) -> x)
                .transform(
                        "test", BasicTypeInfo.LONG_TYPE_INFO, new YieldingTestOperatorFactory<>())
                .sinkTo(new DiscardingSink<>());

        final JobGraph jobGraph = createJobGraph(chainEnv.getStreamGraph());

        final List<JobVertex> vertices = jobGraph.getVerticesSortedTopologicallyFromSources();
        assertThat(vertices).hasSize(1);
        assertThat(vertices.get(0).getOperatorIDs()).hasSize(4);
    }

    @Test
    void testDeterministicUnionOrder() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);

        JobGraph jobGraph = getUnionJobGraph(env);
        JobVertex jobSink = Iterables.getLast(jobGraph.getVerticesSortedTopologicallyFromSources());
        List<String> expectedSourceOrder =
                jobSink.getInputs().stream()
                        .map(edge -> edge.getSource().getProducer().getName())
                        .collect(Collectors.toList());

        for (int i = 0; i < 100; i++) {
            JobGraph jobGraph2 = getUnionJobGraph(env);
            JobVertex jobSink2 =
                    Iterables.getLast(jobGraph2.getVerticesSortedTopologicallyFromSources());
            assertThat(jobSink)
                    .withFailMessage("Different runs should yield different vertexes")
                    .isNotEqualTo(jobSink2);
            List<String> actualSourceOrder =
                    jobSink2.getInputs().stream()
                            .map(edge -> edge.getSource().getProducer().getName())
                            .collect(Collectors.toList());
            assertThat(actualSourceOrder)
                    .withFailMessage("Union inputs reordered")
                    .isEqualTo(expectedSourceOrder);
        }
    }

    private JobGraph getUnionJobGraph(StreamExecutionEnvironment env) {

        createSource(env, 1)
                .union(createSource(env, 2))
                .union(createSource(env, 3))
                .union(createSource(env, 4))
                .sinkTo(new DiscardingSink<>());

        return createJobGraph(env.getStreamGraph());
    }

    private DataStream<Integer> createSource(StreamExecutionEnvironment env, int index) {
        return env.fromData(index).name("source" + index).map(i -> i).name("map" + index);
    }

    @Test
    void testNotSupportInputSelectableOperatorIfCheckpointing() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60_000L);

        DataStreamSource<String> source1 = env.fromData("1");
        DataStreamSource<Integer> source2 = env.fromData(1);
        source1.connect(source2)
                .transform(
                        "test",
                        BasicTypeInfo.STRING_TYPE_INFO,
                        new TestAnyModeReadingStreamOperator("test operator"))
                .print();

        assertThatThrownBy(() -> createJobGraph(env.getStreamGraph()))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testManagedMemoryFractionForUnknownResourceSpec() throws Exception {
        final ResourceSpec resource = ResourceSpec.UNKNOWN;
        final List<ResourceSpec> resourceSpecs =
                Arrays.asList(resource, resource, resource, resource);

        final Configuration taskManagerConfig =
                new Configuration() {
                    {
                        set(
                                TaskManagerOptions.MANAGED_MEMORY_CONSUMER_WEIGHTS,
                                new HashMap<String, String>() {
                                    {
                                        put(
                                                TaskManagerOptions
                                                        .MANAGED_MEMORY_CONSUMER_NAME_OPERATOR,
                                                "6");
                                        put(
                                                TaskManagerOptions
                                                        .MANAGED_MEMORY_CONSUMER_NAME_PYTHON,
                                                "4");
                                    }
                                });
                    }
                };

        final List<Map<ManagedMemoryUseCase, Integer>> operatorScopeManagedMemoryUseCaseWeights =
                new ArrayList<>();
        final List<Set<ManagedMemoryUseCase>> slotScopeManagedMemoryUseCases = new ArrayList<>();

        // source: batch
        operatorScopeManagedMemoryUseCaseWeights.add(
                Collections.singletonMap(ManagedMemoryUseCase.OPERATOR, 1));
        slotScopeManagedMemoryUseCases.add(Collections.emptySet());

        // map1: batch, python
        operatorScopeManagedMemoryUseCaseWeights.add(
                Collections.singletonMap(ManagedMemoryUseCase.OPERATOR, 1));
        slotScopeManagedMemoryUseCases.add(Collections.singleton(ManagedMemoryUseCase.PYTHON));

        // map2: python
        operatorScopeManagedMemoryUseCaseWeights.add(Collections.emptyMap());
        slotScopeManagedMemoryUseCases.add(Collections.singleton(ManagedMemoryUseCase.PYTHON));

        // map3: batch
        operatorScopeManagedMemoryUseCaseWeights.add(
                Collections.singletonMap(ManagedMemoryUseCase.OPERATOR, 1));
        slotScopeManagedMemoryUseCases.add(Collections.emptySet());

        // slotSharingGroup1 contains batch and python use cases: v1(source[batch]) -> map1[batch,
        // python]), v2(map2[python])
        // slotSharingGroup2 contains batch use case only: v3(map3[batch])
        final JobGraph jobGraph =
                createJobGraph(
                        createStreamGraphForManagedMemoryFractionTest(
                                resourceSpecs,
                                operatorScopeManagedMemoryUseCaseWeights,
                                slotScopeManagedMemoryUseCases));
        final JobVertex vertex1 = jobGraph.getVerticesSortedTopologicallyFromSources().get(0);
        final JobVertex vertex2 = jobGraph.getVerticesSortedTopologicallyFromSources().get(1);
        final JobVertex vertex3 = jobGraph.getVerticesSortedTopologicallyFromSources().get(2);
        verifyManagedMemoryFractionForUnknownResourceSpec(
                vertex1, vertex2, vertex3, taskManagerConfig);
    }

    abstract void verifyManagedMemoryFractionForUnknownResourceSpec(
            JobVertex vertex1,
            JobVertex vertex2,
            JobVertex vertex3,
            Configuration taskManagerConfig);

    public static StreamGraph createStreamGraphForManagedMemoryFractionTest(
            final List<ResourceSpec> resourceSpecs,
            final List<Map<ManagedMemoryUseCase, Integer>> operatorScopeUseCaseWeights,
            final List<Set<ManagedMemoryUseCase>> slotScopeUseCases)
            throws Exception {

        final Method opMethod =
                getSetResourcesMethodAndSetAccessible(SingleOutputStreamOperator.class);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final DataStream<Integer> source =
                env.addSource(
                        new ParallelSourceFunction<Integer>() {
                            @Override
                            public void run(SourceContext<Integer> ctx) {}

                            @Override
                            public void cancel() {}
                        });
        opMethod.invoke(source, resourceSpecs.get(0));

        // CHAIN(source -> map1) in default slot sharing group
        final DataStream<Integer> map1 = source.map((MapFunction<Integer, Integer>) value -> value);
        opMethod.invoke(map1, resourceSpecs.get(1));

        // CHAIN(map2) in default slot sharing group
        final DataStream<Integer> map2 =
                map1.rebalance().map((MapFunction<Integer, Integer>) value -> value);
        opMethod.invoke(map2, resourceSpecs.get(2));

        // CHAIN(map3) in test slot sharing group
        final DataStream<Integer> map3 =
                map2.rebalance().map(value -> value).slotSharingGroup("test");
        opMethod.invoke(map3, resourceSpecs.get(3));

        declareManagedMemoryUseCaseForTranformation(
                source.getTransformation(),
                operatorScopeUseCaseWeights.get(0),
                slotScopeUseCases.get(0));
        declareManagedMemoryUseCaseForTranformation(
                map1.getTransformation(),
                operatorScopeUseCaseWeights.get(1),
                slotScopeUseCases.get(1));
        declareManagedMemoryUseCaseForTranformation(
                map2.getTransformation(),
                operatorScopeUseCaseWeights.get(2),
                slotScopeUseCases.get(2));
        declareManagedMemoryUseCaseForTranformation(
                map3.getTransformation(),
                operatorScopeUseCaseWeights.get(3),
                slotScopeUseCases.get(3));

        return env.getStreamGraph();
    }

    private static void declareManagedMemoryUseCaseForTranformation(
            Transformation<?> transformation,
            Map<ManagedMemoryUseCase, Integer> operatorScopeUseCaseWeights,
            Set<ManagedMemoryUseCase> slotScopeUseCases) {
        for (Map.Entry<ManagedMemoryUseCase, Integer> entry :
                operatorScopeUseCaseWeights.entrySet()) {
            transformation.declareManagedMemoryUseCaseAtOperatorScope(
                    entry.getKey(), entry.getValue());
        }
        for (ManagedMemoryUseCase useCase : slotScopeUseCases) {
            transformation.declareManagedMemoryUseCaseAtSlotScope(useCase);
        }
    }

    public static void verifyFractions(
            StreamConfig streamConfig,
            double expectedBatchFrac,
            double expectedPythonFrac,
            double expectedStateBackendFrac,
            Configuration tmConfig) {
        final double delta = 0.000001;
        assertThat(
                        streamConfig.getManagedMemoryFractionOperatorUseCaseOfSlot(
                                ManagedMemoryUseCase.STATE_BACKEND,
                                new Configuration(),
                                tmConfig,
                                ClassLoader.getSystemClassLoader()))
                .isCloseTo(expectedStateBackendFrac, Offset.offset(delta));
        assertThat(
                        streamConfig.getManagedMemoryFractionOperatorUseCaseOfSlot(
                                ManagedMemoryUseCase.PYTHON,
                                new Configuration(),
                                tmConfig,
                                ClassLoader.getSystemClassLoader()))
                .isCloseTo(expectedPythonFrac, Offset.offset(delta));

        assertThat(
                        streamConfig.getManagedMemoryFractionOperatorUseCaseOfSlot(
                                ManagedMemoryUseCase.OPERATOR,
                                new Configuration(),
                                tmConfig,
                                ClassLoader.getSystemClassLoader()))
                .isCloseTo(expectedBatchFrac, Offset.offset(delta));
    }

    @Test
    void testHybridShuffleModeInNonBatchMode() {
        Configuration configuration = new Configuration();
        // set all edge to HYBRID_FULL result partition type.
        configuration.set(
                ExecutionOptions.BATCH_SHUFFLE_MODE, BatchShuffleMode.ALL_EXCHANGES_HYBRID_FULL);
        configuration.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.STREAMING);
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.disableOperatorChaining();
        DataStreamSource<Integer> source = env.fromData(1, 2, 3);
        final DataStream<Integer> partitioned =
                new DataStream<>(
                        env,
                        new PartitionTransformation<>(
                                source.getTransformation(),
                                new RebalancePartitioner<>(),
                                StreamExchangeMode.HYBRID_FULL));
        partitioned.sinkTo(new DiscardingSink<>());
        StreamGraph streamGraph = env.getStreamGraph();
        assertThatThrownBy(() -> createJobGraph(streamGraph))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testSetNonDefaultSlotSharingInHybridMode() {
        Configuration configuration = new Configuration();
        // set all edge to HYBRID_FULL result partition type.
        configuration.set(
                ExecutionOptions.BATCH_SHUFFLE_MODE, BatchShuffleMode.ALL_EXCHANGES_HYBRID_FULL);

        final StreamGraph streamGraph = createStreamGraphForSlotSharingTest(configuration);
        // specify slot sharing group for map1
        streamGraph.getStreamNodes().stream()
                .filter(n -> "map1".equals(n.getOperatorName()))
                .findFirst()
                .get()
                .setSlotSharingGroup("testSlotSharingGroup");
        assertThatThrownBy(() -> createJobGraph(streamGraph))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "hybrid shuffle mode currently does not support setting non-default slot sharing group.");

        // set all edge to HYBRID_SELECTIVE result partition type.
        configuration.set(
                ExecutionOptions.BATCH_SHUFFLE_MODE,
                BatchShuffleMode.ALL_EXCHANGES_HYBRID_SELECTIVE);

        final StreamGraph streamGraph2 = createStreamGraphForSlotSharingTest(configuration);
        // specify slot sharing group for map1
        streamGraph2.getStreamNodes().stream()
                .filter(n -> "map1".equals(n.getOperatorName()))
                .findFirst()
                .get()
                .setSlotSharingGroup("testSlotSharingGroup");
        assertThatThrownBy(() -> createJobGraph(streamGraph2))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "hybrid shuffle mode currently does not support setting non-default slot sharing group.");
    }

    @Test
    void testSlotSharingOnAllVerticesInSameSlotSharingGroupByDefaultEnabled() {
        final StreamGraph streamGraph = createStreamGraphForSlotSharingTest(new Configuration());
        // specify slot sharing group for map1
        streamGraph.getStreamNodes().stream()
                .filter(n -> "map1".equals(n.getOperatorName()))
                .findFirst()
                .get()
                .setSlotSharingGroup("testSlotSharingGroup");
        streamGraph.setAllVerticesInSameSlotSharingGroupByDefault(true);
        final JobGraph jobGraph = createJobGraph(streamGraph);

        final List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();
        assertThat(verticesSorted).hasSize(4);

        final List<JobVertex> verticesMatched = getExpectedVerticesList(verticesSorted);
        final JobVertex source1Vertex = verticesMatched.get(0);
        final JobVertex source2Vertex = verticesMatched.get(1);
        final JobVertex map1Vertex = verticesMatched.get(2);
        final JobVertex map2Vertex = verticesMatched.get(3);

        // all vertices should be in the same default slot sharing group
        // except for map1 which has a specified slot sharing group
        assertSameSlotSharingGroup(source1Vertex, source2Vertex, map2Vertex);
        assertDistinctSharingGroups(source1Vertex, map1Vertex);
    }

    @Test
    void testSlotSharingOnAllVerticesInSameSlotSharingGroupByDefaultDisabled() {
        final StreamGraph streamGraph = createStreamGraphForSlotSharingTest(new Configuration());
        streamGraph.setAllVerticesInSameSlotSharingGroupByDefault(false);
        final JobGraph jobGraph = createJobGraph(streamGraph);

        final List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();
        assertThat(verticesSorted).hasSize(4);

        final List<JobVertex> verticesMatched = getExpectedVerticesList(verticesSorted);
        final JobVertex source1Vertex = verticesMatched.get(0);
        final JobVertex source2Vertex = verticesMatched.get(1);
        final JobVertex map1Vertex = verticesMatched.get(2);
        final JobVertex map2Vertex = verticesMatched.get(3);

        // vertices in different regions should be in different slot sharing groups
        assertDistinctSharingGroups(source1Vertex, source2Vertex, map2Vertex, map1Vertex);
    }

    @Test
    void testSlotSharingResourceConfiguration() {
        final String slotSharingGroup1 = "slot-a";
        final String slotSharingGroup2 = "slot-b";
        final ResourceProfile resourceProfile1 = ResourceProfile.fromResources(1, 10);
        final ResourceProfile resourceProfile2 = ResourceProfile.fromResources(2, 20);
        final ResourceProfile resourceProfile3 = ResourceProfile.fromResources(3, 30);
        final Map<String, ResourceProfile> slotSharingGroupResource = new HashMap<>();
        slotSharingGroupResource.put(slotSharingGroup1, resourceProfile1);
        slotSharingGroupResource.put(slotSharingGroup2, resourceProfile2);
        slotSharingGroupResource.put(
                StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP, resourceProfile3);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromData(1, 2, 3)
                .name(slotSharingGroup1)
                .slotSharingGroup(slotSharingGroup1)
                .map(x -> x + 1)
                .name(slotSharingGroup2)
                .slotSharingGroup(slotSharingGroup2)
                .map(x -> x * x)
                .name(StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP)
                .slotSharingGroup(StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP);

        final StreamGraph streamGraph = env.getStreamGraph();
        streamGraph.setSlotSharingGroupResource(slotSharingGroupResource);
        final JobGraph jobGraph = createJobGraph(streamGraph);

        int numVertex = 0;
        for (JobVertex jobVertex : jobGraph.getVertices()) {
            numVertex += 1;
            if (jobVertex.getName().contains(slotSharingGroup1)) {
                assertThat(jobVertex.getSlotSharingGroup().getResourceProfile())
                        .isEqualTo(resourceProfile1);
            } else if (jobVertex.getName().contains(slotSharingGroup2)) {
                assertThat(jobVertex.getSlotSharingGroup().getResourceProfile())
                        .isEqualTo(resourceProfile2);
            } else if (jobVertex
                    .getName()
                    .contains(StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP)) {
                assertThat(jobVertex.getSlotSharingGroup().getResourceProfile())
                        .isEqualTo(resourceProfile3);
            } else {
                Assertions.fail("");
            }
        }
        assertThat(numVertex).isEqualTo(3);
    }

    @Test
    void testSlotSharingResourceConfigurationWithDefaultSlotSharingGroup() {
        final ResourceProfile resourceProfile = ResourceProfile.fromResources(1, 10);
        final Map<String, ResourceProfile> slotSharingGroupResource = new HashMap<>();
        slotSharingGroupResource.put(
                StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP, resourceProfile);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromData(1, 2, 3).map(x -> x + 1);

        final StreamGraph streamGraph = env.getStreamGraph();
        streamGraph.setSlotSharingGroupResource(slotSharingGroupResource);
        final JobGraph jobGraph = createJobGraph(streamGraph);

        int numVertex = 0;
        for (JobVertex jobVertex : jobGraph.getVertices()) {
            numVertex += 1;
            assertThat(jobVertex.getSlotSharingGroup().getResourceProfile())
                    .isEqualTo(resourceProfile);
        }
        assertThat(numVertex).isEqualTo(2);
    }

    @Test
    void testNamingOfChainedMultipleInputs() {
        String[] sources = new String[] {"source-1", "source-2", "source-3"};
        String sink = "sink";
        JobGraph graph = createGraphWithMultipleInputs(true, sink, sources);
        Iterator<JobVertex> iterator = graph.getVerticesSortedTopologicallyFromSources().iterator();

        JobVertex multipleVertex = iterator.next();
        assertThat(multipleVertex.getName())
                .isEqualTo("mit [Source: source-1, Source: source-2, Source: source-3]");
        assertThat(multipleVertex.getOperatorPrettyName())
                .isEqualTo("mit [Source: source-1, Source: source-2, Source: source-3]\n");

        JobVertex sinkVertex = iterator.next();
        assertThat(sinkVertex.getName()).isEqualTo("sink: Writer");
        assertThat(sinkVertex.getOperatorPrettyName()).isEqualTo("sink: Writer\n");
    }

    @Test
    void testNamingOfNonChainedMultipleInputs() {
        String[] sources = new String[] {"source-1", "source-2", "source-3"};
        String sink = "sink";
        JobGraph graph = createGraphWithMultipleInputs(false, sink, sources);
        Iterator<JobVertex> iterator = graph.getVerticesSortedTopologicallyFromSources().iterator();

        JobVertex source1 = iterator.next();
        assertThat(source1.getName()).isEqualTo("Source: source-1");
        assertThat(source1.getOperatorPrettyName()).isEqualTo("Source: source-1\n");

        JobVertex source2 = iterator.next();
        assertThat(source2.getName()).isEqualTo("Source: source-2");
        assertThat(source2.getOperatorPrettyName()).isEqualTo("Source: source-2\n");

        JobVertex source3 = iterator.next();
        assertThat(source3.getName()).isEqualTo("Source: source-3");
        assertThat(source3.getOperatorPrettyName()).isEqualTo("Source: source-3\n");

        JobVertex multipleVertex = iterator.next();
        assertThat(multipleVertex.getName()).isEqualTo("mit");
        assertThat(multipleVertex.getOperatorPrettyName()).isEqualTo("mit\n");

        JobVertex sinkVertex = iterator.next();
        assertThat(sinkVertex.getName()).isEqualTo("sink: Writer");
        assertThat(sinkVertex.getOperatorPrettyName()).isEqualTo("sink: Writer\n");
    }

    public JobGraph createGraphWithMultipleInputs(
            boolean chain, String sinkName, String... inputNames) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        MultipleInputTransformation<Long> transform =
                new MultipleInputTransformation<>(
                        "mit", new UnusedOperatorFactory(), Types.LONG, env.getParallelism());
        Arrays.stream(inputNames)
                .map(
                        name ->
                                env.fromSource(
                                                new NumberSequenceSource(1, 2),
                                                WatermarkStrategy.noWatermarks(),
                                                name)
                                        .getTransformation())
                .forEach(transform::addInput);
        transform.setChainingStrategy(
                chain ? ChainingStrategy.HEAD_WITH_SOURCES : ChainingStrategy.NEVER);

        DataStream<Long> dataStream = new DataStream<>(env, transform);
        // do not chain with sink operator.
        dataStream.rebalance().sinkTo(new DiscardingSink<>()).name(sinkName);

        env.addOperator(transform);

        return createJobGraph(env.getStreamGraph());
    }

    @Test
    void testTreeDescription() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        JobGraph job = createJobGraphWithDescription(env, "test source");
        JobVertex[] allVertices = job.getVerticesAsArray();
        assertThat(allVertices).hasSize(1);
        assertThat(allVertices[0].getOperatorPrettyName())
                .isEqualTo(
                        "test source\n"
                                + ":- x + 1\n"
                                + ":  :- first print of map1\n"
                                + ":  +- second print of map1\n"
                                + "+- x + 2\n"
                                + "   :- first print of map2\n"
                                + "   +- second print of map2\n");
    }

    @Test
    void testTreeDescriptionWithChainedSource() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        JobGraph job = createJobGraphWithDescription(env, "test source 1", "test source 2");
        JobVertex[] allVertices = job.getVerticesAsArray();
        assertThat(allVertices).hasSize(1);
        assertThat(allVertices[0].getOperatorPrettyName())
                .isEqualTo(
                        "operator chained with source [test source 1, test source 2]\n"
                                + ":- x + 1\n"
                                + ":  :- first print of map1\n"
                                + ":  +- second print of map1\n"
                                + "+- x + 2\n"
                                + "   :- first print of map2\n"
                                + "   +- second print of map2\n");
    }

    @Test
    void testCascadingDescription() {
        final Configuration config = new Configuration();
        config.set(
                PipelineOptions.VERTEX_DESCRIPTION_MODE,
                PipelineOptions.VertexDescriptionMode.CASCADING);
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(config);
        JobGraph job = createJobGraphWithDescription(env, "test source");
        JobVertex[] allVertices = job.getVerticesAsArray();
        assertThat(allVertices).hasSize(1);
        assertThat(allVertices[0].getOperatorPrettyName())
                .isEqualTo(
                        "test source -> (x + 1 -> (first print of map1 , second print of map1) , x + 2 -> (first print of map2 , second print of map2))");
    }

    @Test
    void testCascadingDescriptionWithChainedSource() {
        final Configuration config = new Configuration();
        config.set(
                PipelineOptions.VERTEX_DESCRIPTION_MODE,
                PipelineOptions.VertexDescriptionMode.CASCADING);
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(config);
        JobGraph job = createJobGraphWithDescription(env, "test source 1", "test source 2");
        JobVertex[] allVertices = job.getVerticesAsArray();
        assertThat(allVertices).hasSize(1);
        assertThat(allVertices[0].getOperatorPrettyName())
                .isEqualTo(
                        "operator chained with source [test source 1, test source 2] -> (x + 1 -> (first print of map1 , second print of map1) , x + 2 -> (first print of map2 , second print of map2))");
    }

    @Test
    void testNamingWithoutIndex() {
        JobGraph job = createJobGraph(createStreamGraphForSlotSharingTest(new Configuration()));
        List<JobVertex> allVertices = job.getVerticesSortedTopologicallyFromSources();
        assertThat(allVertices).hasSize(4);
        assertThat(allVertices.get(0).getName()).isEqualTo("Source: source1");
        assertThat(allVertices.get(1).getName()).isEqualTo("Source: source2");
        assertThat(allVertices.get(2).getName()).isEqualTo("map1");
        assertThat(allVertices.get(3).getName()).isEqualTo("map2");
    }

    @Test
    void testNamingWithIndex() {
        Configuration config = new Configuration();
        config.set(PipelineOptions.VERTEX_NAME_INCLUDE_INDEX_PREFIX, true);
        JobGraph job = createJobGraph(createStreamGraphForSlotSharingTest(config));
        List<JobVertex> allVertices = job.getVerticesSortedTopologicallyFromSources();
        assertThat(allVertices).hasSize(4);
        assertThat(allVertices.get(0).getName()).isEqualTo("[vertex-0]Source: source1");
        assertThat(allVertices.get(1).getName()).isEqualTo("[vertex-1]Source: source2");
        assertThat(allVertices.get(2).getName()).isEqualTo("[vertex-2]map1");
        assertThat(allVertices.get(3).getName()).isEqualTo("[vertex-3]map2");
    }

    @Test
    void testCacheJobGraph() throws Throwable {
        final TestingStreamExecutionEnvironment env = new TestingStreamExecutionEnvironment();
        env.setParallelism(2);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        DataStream<Integer> source = env.fromData(1, 2, 3).name("source");
        CachedDataStream<Integer> cachedStream =
                source.map(i -> i + 1).name("map-1").map(i -> i + 1).name("map-2").cache();
        assertThat(cachedStream.getTransformation()).isInstanceOf(CacheTransformation.class);
        CacheTransformation<Integer> cacheTransformation =
                (CacheTransformation<Integer>) cachedStream.getTransformation();

        cachedStream.print().name("print");

        JobGraph jobGraph = createJobGraph(env.getStreamGraph());
        List<JobVertex> allVertices = jobGraph.getVerticesSortedTopologicallyFromSources();
        assertThat(allVertices).hasSize(3);

        final JobVertex cacheWriteVertex =
                allVertices.stream()
                        .filter(jobVertex -> "CacheWrite".equals(jobVertex.getName()))
                        .findFirst()
                        .orElseThrow(
                                (Supplier<Throwable>)
                                        () ->
                                                new RuntimeException(
                                                        "CacheWrite job vertex not found"));

        final List<JobEdge> inputs = cacheWriteVertex.getInputs();
        assertThat(inputs).hasSize(1);
        assertThat(inputs.get(0).getDistributionPattern()).isEqualTo(POINTWISE);
        assertThat(inputs.get(0).getSource().getResultType())
                .isEqualTo(ResultPartitionType.BLOCKING_PERSISTENT);
        assertThat(new AbstractID(inputs.get(0).getSourceId()))
                .isEqualTo(cacheTransformation.getDatasetId());
        assertThat(inputs.get(0).getSource().getProducer().getName())
                .isEqualTo("map-1 -> map-2 -> Sink: print");

        env.addCompletedClusterDataset(cacheTransformation.getDatasetId());
        cachedStream.print().name("print");

        jobGraph = createJobGraph(env.getStreamGraph());
        allVertices = jobGraph.getVerticesSortedTopologicallyFromSources();
        assertThat(allVertices).hasSize(1);
        assertThat(allVertices.get(0).getName()).isEqualTo("CacheRead -> Sink: print");
        assertThat(allVertices.get(0).getIntermediateDataSetIdsToConsume()).hasSize(1);
        assertThat(new AbstractID(allVertices.get(0).getIntermediateDataSetIdsToConsume().get(0)))
                .isEqualTo(cacheTransformation.getDatasetId());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testHybridBroadcastEdgeAlwaysUseFullResultPartition(boolean isSelective) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.disableOperatorChaining();

        DataStream<Integer> sourceDataStream = env.fromData(1, 2, 3);
        DataStream<Integer> partitionAfterSourceDataStream =
                new DataStream<>(
                        env,
                        new PartitionTransformation<>(
                                sourceDataStream.getTransformation(),
                                new BroadcastPartitioner<>(),
                                isSelective
                                        ? StreamExchangeMode.HYBRID_SELECTIVE
                                        : StreamExchangeMode.HYBRID_FULL));
        partitionAfterSourceDataStream.sinkTo(new DiscardingSink<>());

        JobGraph jobGraph = createJobGraph(env.getStreamGraph());

        List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();
        assertThat(verticesSorted).hasSize(2);

        JobVertex sourceAndMapVertex = verticesSorted.get(0);
        assertThat(sourceAndMapVertex.getProducedDataSets().get(0).getResultType())
                .isEqualTo(ResultPartitionType.HYBRID_FULL);
    }

    @Test
    void testHybridPartitionReuse() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        DataStream<Integer> source = env.fromData(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        DataStream<Integer> partition1 =
                new DataStream<>(
                        env,
                        new PartitionTransformation<>(
                                source.getTransformation(),
                                new RebalancePartitioner<>(),
                                StreamExchangeMode.HYBRID_FULL));
        DataStream<Integer> partition2 =
                new DataStream<>(
                        env,
                        new PartitionTransformation<>(
                                source.getTransformation(),
                                new RebalancePartitioner<>(),
                                StreamExchangeMode.HYBRID_SELECTIVE));
        DataStream<Integer> partition3 =
                new DataStream<>(
                        env,
                        new PartitionTransformation<>(
                                source.getTransformation(),
                                new RebalancePartitioner<>(),
                                StreamExchangeMode.HYBRID_SELECTIVE));
        DataStream<Integer> partition4 =
                new DataStream<>(
                        env,
                        new PartitionTransformation<>(
                                source.getTransformation(),
                                new RescalePartitioner<>(),
                                StreamExchangeMode.HYBRID_FULL));
        DataStream<Integer> partition5 =
                new DataStream<>(
                        env,
                        new PartitionTransformation<>(
                                source.getTransformation(),
                                new RescalePartitioner<>(),
                                StreamExchangeMode.BATCH));
        DataStream<Integer> partition7 =
                new DataStream<>(
                        env,
                        new PartitionTransformation<>(
                                source.getTransformation(),
                                new ForwardPartitioner<>(),
                                StreamExchangeMode.HYBRID_FULL));
        // these two vertices can reuse the same intermediate dataset
        partition1.sinkTo(new DiscardingSink<>()).setParallelism(2).name("sink1");
        partition2.sinkTo(new DiscardingSink<>()).setParallelism(2).name("sink2");

        // this can not reuse the same intermediate dataset because of different parallelism
        partition3.sinkTo(new DiscardingSink<>()).setParallelism(3);

        // this can not reuse the same intermediate dataset because of different partitioner
        partition4.sinkTo(new DiscardingSink<>()).setParallelism(2);

        // this can not reuse the same intermediate dataset because of different result partition
        // type
        partition5.sinkTo(new DiscardingSink<>()).setParallelism(2);

        SingleOutputStreamOperator<Integer> mapStream =
                partition7.map(value -> value).setParallelism(1);
        DataStream<Integer> mapPartition =
                new DataStream<>(
                        env,
                        new PartitionTransformation<>(
                                mapStream.getTransformation(),
                                new RescalePartitioner<>(),
                                StreamExchangeMode.HYBRID_FULL));
        mapPartition.sinkTo(new DiscardingSink<>()).name("sink3");

        StreamGraph streamGraph = env.getStreamGraph();
        JobGraph jobGraph = createJobGraph(streamGraph);

        List<JobVertex> vertices = jobGraph.getVerticesSortedTopologicallyFromSources();
        assertThat(vertices).hasSize(7);

        JobVertex sourceVertex = vertices.get(0);
        List<IntermediateDataSetID> producedDataSet =
                sourceVertex.getProducedDataSets().stream()
                        .map(IntermediateDataSet::getId)
                        .collect(Collectors.toList());
        assertThat(producedDataSet).hasSize(5);

        JobVertex sinkVertex1 = checkNotNull(findJobVertexWithName(vertices, "sink1"));
        JobVertex sinkVertex2 = checkNotNull(findJobVertexWithName(vertices, "sink2"));
        JobVertex sinkVertex3 = checkNotNull(findJobVertexWithName(vertices, "sink3"));

        assertThat(sinkVertex2.getInputs().get(0).getSource().getId())
                .isEqualTo(sinkVertex1.getInputs().get(0).getSource().getId());
        assertThat(sinkVertex3.getInputs().get(0).getSource().getId())
                .isNotEqualTo(sinkVertex1.getInputs().get(0).getSource().getId());

        StreamConfig streamConfig = new StreamConfig(sourceVertex.getConfiguration());
        List<IntermediateDataSetID> nonChainedOutputs =
                streamConfig.getOperatorNonChainedOutputs(getClass().getClassLoader()).stream()
                        .map(NonChainedOutput::getDataSetId)
                        .collect(Collectors.toList());
        assertThat(nonChainedOutputs).hasSize(4);

        List<IntermediateDataSetID> streamOutputsInOrder =
                streamConfig.getVertexNonChainedOutputs(getClass().getClassLoader()).stream()
                        .map(NonChainedOutput::getDataSetId)
                        .collect(Collectors.toList());
        assertThat(streamOutputsInOrder).hasSize(5);
        assertThat(streamOutputsInOrder).isEqualTo(producedDataSet);
    }

    /**
     * Tests that multiple downstream consumer vertices can reuse the same intermediate blocking
     * dataset if they have the same parallelism and partitioner.
     */
    @Test
    void testIntermediateDataSetReuse() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setBufferTimeout(-1);
        DataStream<Integer> source = env.fromData(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        // these two vertices can reuse the same intermediate dataset
        source.rebalance().sinkTo(new DiscardingSink<>()).setParallelism(2).name("sink1");
        source.rebalance().sinkTo(new DiscardingSink<>()).setParallelism(2).name("sink2");

        // this can not reuse the same intermediate dataset because of different parallelism
        source.rebalance().sinkTo(new DiscardingSink<>()).setParallelism(3);

        // this can not reuse the same intermediate dataset because of different partitioner
        source.broadcast().sinkTo(new DiscardingSink<>()).setParallelism(2);

        // these two vertices can not reuse the same intermediate dataset because of the pipelined
        // edge
        source.forward().sinkTo(new DiscardingSink<>()).setParallelism(1).disableChaining();
        source.forward().sinkTo(new DiscardingSink<>()).setParallelism(1).disableChaining();

        DataStream<Integer> mapStream = source.forward().map(value -> value).setParallelism(1);

        // these two vertices can reuse the same intermediate dataset
        mapStream.broadcast().sinkTo(new DiscardingSink<>()).setParallelism(2).name("sink3");
        mapStream.broadcast().sinkTo(new DiscardingSink<>()).setParallelism(2).name("sink4");

        StreamGraph streamGraph = env.getStreamGraph();
        streamGraph.setGlobalStreamExchangeMode(GlobalStreamExchangeMode.FORWARD_EDGES_PIPELINED);
        JobGraph jobGraph = createJobGraph(streamGraph);

        List<JobVertex> vertices = jobGraph.getVerticesSortedTopologicallyFromSources();
        assertThat(vertices).hasSize(9);

        JobVertex sourceVertex = vertices.get(0);
        List<IntermediateDataSetID> producedDataSet =
                sourceVertex.getProducedDataSets().stream()
                        .map(IntermediateDataSet::getId)
                        .collect(Collectors.toList());
        assertThat(producedDataSet).hasSize(6);

        JobVertex sinkVertex1 = checkNotNull(findJobVertexWithName(vertices, "sink1"));
        JobVertex sinkVertex2 = checkNotNull(findJobVertexWithName(vertices, "sink2"));
        JobVertex sinkVertex3 = checkNotNull(findJobVertexWithName(vertices, "sink3"));
        JobVertex sinkVertex4 = checkNotNull(findJobVertexWithName(vertices, "sink4"));

        assertThat(sinkVertex2.getInputs().get(0).getSource().getId())
                .isEqualTo(sinkVertex1.getInputs().get(0).getSource().getId());
        assertThat(sinkVertex4.getInputs().get(0).getSource().getId())
                .isEqualTo(sinkVertex3.getInputs().get(0).getSource().getId());
        assertThat(sinkVertex3.getInputs().get(0).getSource().getId())
                .isNotEqualTo(sinkVertex1.getInputs().get(0).getSource().getId());

        StreamConfig streamConfig = new StreamConfig(sourceVertex.getConfiguration());
        List<IntermediateDataSetID> nonChainedOutputs =
                streamConfig.getOperatorNonChainedOutputs(getClass().getClassLoader()).stream()
                        .map(NonChainedOutput::getDataSetId)
                        .collect(Collectors.toList());
        assertThat(nonChainedOutputs).hasSize(5);
        assertThat(nonChainedOutputs)
                .doesNotContain(sinkVertex3.getInputs().get(0).getSource().getId());

        List<IntermediateDataSetID> streamOutputsInOrder =
                streamConfig.getVertexNonChainedOutputs(getClass().getClassLoader()).stream()
                        .map(NonChainedOutput::getDataSetId)
                        .collect(Collectors.toList());
        assertThat(streamOutputsInOrder).hasSize(6);
        assertThat(streamOutputsInOrder).isEqualTo(producedDataSet);
    }

    @Test
    void testStreamConfigSerializationException() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> source = env.fromData(1, 2, 3);
        env.addOperator(
                new OneInputTransformation<>(
                        source.getTransformation(),
                        "serializationTestOperator",
                        // using a non-serializable operator factory to trigger an IOException when
                        // try to serialize streamConfig.
                        new SerializationTestOperatorFactory(false),
                        Types.INT,
                        1));
        final StreamGraph streamGraph = env.getStreamGraph();
        assertThatThrownBy(() -> createJobGraph(streamGraph))
                .hasRootCauseInstanceOf(IOException.class)
                .hasRootCauseMessage("This operator factory is not serializable.");
    }

    @Test
    void testCoordinatedSerializationException() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> source = env.fromData(1, 2, 3);
        env.addOperator(
                new OneInputTransformation<>(
                        source.getTransformation(),
                        "serializationTestOperator",
                        new SerializationTestOperatorFactory(true),
                        Types.INT,
                        1));

        StreamGraph streamGraph = env.getStreamGraph();
        assertThatThrownBy(() -> createJobGraph(streamGraph))
                .hasRootCauseInstanceOf(IOException.class)
                .hasRootCauseMessage("This provider is not serializable.");
    }

    @Test
    void testSinkWithAllInterfaces() {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(new Configuration());
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        final DataStream<Integer> source = env.fromData(1, 2, 3).name("source");
        source.rebalance().sinkTo(new TestSinkWithAllInterfaces()).name("sink");

        final StreamGraph streamGraph = env.getStreamGraph();
        final JobGraph jobGraph = createJobGraph(streamGraph);
        assertThat(jobGraph.getNumberOfVertices()).isEqualTo(6);
        for (JobVertex jobVertex : jobGraph.getVertices()) {
            assertThat(jobVertex.getName())
                    .containsAnyOf(
                            "source",
                            "pre-writer",
                            "Writer",
                            "pre-committer",
                            "post-committer",
                            "Committer");
        }
    }

    @Test
    void testSupportConcurrentExecutionAttempts() {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(new Configuration());
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        final DataStream<Integer> source = env.fromData(1, 2, 3).name("source");
        // source -> (map1 -> map2) -> sink
        source.rebalance()
                .map(v -> v)
                .name("map1")
                .map(v -> v)
                .name("map2")
                .rebalance()
                .sinkTo(new PrintSink<>())
                .name("sink");

        final StreamGraph streamGraph = env.getStreamGraph();
        final List<StreamNode> streamNodes =
                streamGraph.getStreamNodes().stream()
                        .sorted(Comparator.comparingInt(StreamNode::getId))
                        .collect(Collectors.toList());

        final StreamNode sourceNode = streamNodes.get(0);
        final StreamNode map1Node = streamNodes.get(1);
        final StreamNode map2Node = streamNodes.get(2);
        final StreamNode sinkNode = streamNodes.get(3);
        streamGraph.setSupportsConcurrentExecutionAttempts(sourceNode.getId(), true);
        // map1 and map2 are chained
        // map1 supports concurrent execution attempt however map2 does not
        streamGraph.setSupportsConcurrentExecutionAttempts(map1Node.getId(), true);
        streamGraph.setSupportsConcurrentExecutionAttempts(map2Node.getId(), false);
        streamGraph.setSupportsConcurrentExecutionAttempts(sinkNode.getId(), false);

        final JobGraph jobGraph = createJobGraph(streamGraph);
        assertThat(jobGraph.getNumberOfVertices()).isEqualTo(3);
        for (JobVertex jobVertex : jobGraph.getVertices()) {
            if (jobVertex.getName().contains("source")) {
                assertThat(jobVertex.isSupportsConcurrentExecutionAttempts()).isTrue();
            } else if (jobVertex.getName().contains("map")) {
                // chained job vertex does not support concurrent execution attempt if any operator
                // in chain does not support it
                assertThat(jobVertex.isSupportsConcurrentExecutionAttempts()).isFalse();
            } else if (jobVertex.getName().contains("sink")) {
                assertThat(jobVertex.isSupportsConcurrentExecutionAttempts()).isFalse();
            } else {
                Assertions.fail("Unexpected job vertex " + jobVertex.getName());
            }
        }
    }

    @Test
    void testSinkSupportConcurrentExecutionAttempts() {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(new Configuration());
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        final DataStream<Integer> source = env.fromData(1, 2, 3).name("source");
        source.rebalance()
                .sinkTo(new TestSinkWithSupportsConcurrentExecutionAttempts())
                .name("sink");

        final StreamGraph streamGraph = env.getStreamGraph();
        final JobGraph jobGraph = createJobGraph(streamGraph);
        assertThat(jobGraph.getNumberOfVertices()).isEqualTo(6);
        for (JobVertex jobVertex : jobGraph.getVertices()) {
            if (jobVertex.getName().contains("source")) {
                assertThat(jobVertex.isSupportsConcurrentExecutionAttempts()).isTrue();
            } else if (jobVertex.getName().contains("pre-writer")) {
                assertThat(jobVertex.isSupportsConcurrentExecutionAttempts()).isTrue();
            } else if (jobVertex.getName().contains("Writer")) {
                assertThat(jobVertex.isSupportsConcurrentExecutionAttempts()).isTrue();
            } else if (jobVertex.getName().contains("pre-committer")) {
                assertThat(jobVertex.isSupportsConcurrentExecutionAttempts()).isFalse();
            } else if (jobVertex.getName().contains("post-committer")) {
                assertThat(jobVertex.isSupportsConcurrentExecutionAttempts()).isFalse();
            } else if (jobVertex.getName().contains("Committer")) {
                assertThat(jobVertex.isSupportsConcurrentExecutionAttempts()).isFalse();
            } else {
                Assertions.fail("Unexpected job vertex " + jobVertex.getName());
            }
        }
    }

    @Test
    void testSinkFunctionNotSupportConcurrentExecutionAttempts() {
        testWhetherSinkFunctionSupportsConcurrentExecutionAttempts(
                new TestingSinkFunctionNotSupportConcurrentExecutionAttempts<>(), false);
    }

    @Test
    void testSinkFunctionSupportConcurrentExecutionAttempts() {
        testWhetherSinkFunctionSupportsConcurrentExecutionAttempts(
                new TestingSinkFunctionSupportConcurrentExecutionAttempts<>(), true);
    }

    @Test
    void testOutputFormatNotSupportConcurrentExecutionAttempts() {
        testWhetherOutputFormatSupportsConcurrentExecutionAttempts(
                new TestingOutputFormatNotSupportConcurrentExecutionAttempts<>(), false);
    }

    @Test
    void testOutputFormatSupportConcurrentExecutionAttempts() {
        testWhetherOutputFormatSupportsConcurrentExecutionAttempts(
                new TestingOutputFormatSupportConcurrentExecutionAttempts<>(), true);
    }

    @Test
    void testEnableAsyncStateForSyncOperatorThrowException() throws Exception {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(new Configuration());
        try {
            env.fromData(1, 2, 3, 4, 5)
                    .keyBy(k -> k)
                    .flatMap(
                            new FlatMapFunction<Integer, Integer>() {
                                @Override
                                public void flatMap(Integer value, Collector<Integer> out)
                                        throws Exception {
                                    out.collect(value);
                                }
                            })
                    .enableAsyncState()
                    .print();
            fail("Enabling async state for synchronous operators is forbidden.");
        } catch (UnsupportedOperationException e) {
            assertThat(e.getMessage())
                    .isEqualTo(
                            "The transformation does not support "
                                    + "async state, or you are enabling the async state without a keyed context (not behind a keyBy()).");
        }
    }

    private void testWhetherOutputFormatSupportsConcurrentExecutionAttempts(
            OutputFormat<Integer> outputFormat, boolean isSupported) {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(new Configuration());
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        final DataStream<Integer> source = env.fromData(1, 2, 3).name("source");
        source.rebalance().writeUsingOutputFormat(outputFormat).name("sink");

        final StreamGraph streamGraph = env.getStreamGraph();
        final JobGraph jobGraph = createJobGraph(streamGraph);
        assertThat(jobGraph.getNumberOfVertices()).isEqualTo(2);
        for (JobVertex jobVertex : jobGraph.getVertices()) {
            if (jobVertex.getName().contains("source")) {
                assertThat(jobVertex.isSupportsConcurrentExecutionAttempts()).isTrue();
            } else if (jobVertex.getName().contains("sink")) {
                if (isSupported) {
                    assertThat(jobVertex.isSupportsConcurrentExecutionAttempts()).isTrue();
                } else {
                    assertThat(jobVertex.isSupportsConcurrentExecutionAttempts()).isFalse();
                }
            } else {
                Assertions.fail("Unexpected job vertex " + jobVertex.getName());
            }
        }
    }

    private void testWhetherSinkFunctionSupportsConcurrentExecutionAttempts(
            SinkFunction<Integer> function, boolean isSupported) {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(new Configuration());
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        final DataStream<Integer> source = env.fromData(1, 2, 3).name("source");
        source.rebalance().addSink(function).name("sink");

        final StreamGraph streamGraph = env.getStreamGraph();
        final JobGraph jobGraph = createJobGraph(streamGraph);
        assertThat(jobGraph.getNumberOfVertices()).isEqualTo(2);
        for (JobVertex jobVertex : jobGraph.getVertices()) {
            if (jobVertex.getName().contains("source")) {
                assertThat(jobVertex.isSupportsConcurrentExecutionAttempts()).isTrue();
            } else if (jobVertex.getName().contains("sink")) {
                if (isSupported) {
                    assertThat(jobVertex.isSupportsConcurrentExecutionAttempts()).isTrue();
                } else {
                    assertThat(jobVertex.isSupportsConcurrentExecutionAttempts()).isFalse();
                }
            } else {
                Assertions.fail("Unexpected job vertex " + jobVertex.getName());
            }
        }
    }

    private static class TestingOutputFormatNotSupportConcurrentExecutionAttempts<T>
            implements OutputFormat<T> {

        @Override
        public void configure(Configuration parameters) {}

        @Override
        public void open(InitializationContext context) throws IOException {}

        @Override
        public void writeRecord(T record) throws IOException {}

        @Override
        public void close() throws IOException {}
    }

    private static class TestingOutputFormatSupportConcurrentExecutionAttempts<T>
            implements OutputFormat<T>, SupportsConcurrentExecutionAttempts {

        @Override
        public void configure(Configuration parameters) {}

        @Override
        public void open(InitializationContext context) throws IOException {}

        @Override
        public void writeRecord(T record) throws IOException {}

        @Override
        public void close() throws IOException {}
    }

    private static class TestingSinkFunctionNotSupportConcurrentExecutionAttempts<T>
            implements SinkFunction<T> {
        @Override
        public void invoke(T value, Context context) throws Exception {}
    }

    private static class TestingSinkFunctionSupportConcurrentExecutionAttempts<T>
            implements SinkFunction<T>, SupportsConcurrentExecutionAttempts {
        @Override
        public void invoke(T value, Context context) throws Exception {}
    }

    private static class TestSinkWithSupportsConcurrentExecutionAttempts
            implements SupportsConcurrentExecutionAttempts,
                    Sink<Integer>,
                    SupportsCommitter<Void>,
                    SupportsPreWriteTopology<Integer>,
                    SupportsPreCommitTopology<Void, Void>,
                    SupportsPostCommitTopology<Void> {

        @Override
        public SinkWriter<Integer> createWriter(WriterInitContext context) throws IOException {
            return new CommittingSinkWriter<Integer, Void>() {
                @Override
                public Collection<Void> prepareCommit() throws IOException, InterruptedException {
                    return null;
                }

                @Override
                public void write(Integer element, Context context)
                        throws IOException, InterruptedException {}

                @Override
                public void flush(boolean endOfInput) throws IOException, InterruptedException {}

                @Override
                public void close() throws Exception {}
            };
        }

        @Override
        public Committer<Void> createCommitter(CommitterInitContext context) throws IOException {
            return new Committer<Void>() {
                @Override
                public void commit(Collection<CommitRequest<Void>> committables)
                        throws IOException, InterruptedException {}

                @Override
                public void close() throws Exception {}
            };
        }

        @Override
        public SimpleVersionedSerializer<Void> getCommittableSerializer() {
            return new SimpleVersionedSerializer<Void>() {
                @Override
                public int getVersion() {
                    return 0;
                }

                @Override
                public byte[] serialize(Void obj) throws IOException {
                    return new byte[0];
                }

                @Override
                public Void deserialize(int version, byte[] serialized) throws IOException {
                    return null;
                }
            };
        }

        @Override
        public void addPostCommitTopology(DataStream<CommittableMessage<Void>> committables) {
            committables
                    .map(v -> v)
                    .name("post-committer")
                    .returns(CommittableMessageTypeInfo.noOutput())
                    .rebalance();
        }

        @Override
        public DataStream<CommittableMessage<Void>> addPreCommitTopology(
                DataStream<CommittableMessage<Void>> committables) {
            return committables
                    .map(v -> v)
                    .name("pre-committer")
                    .returns(CommittableMessageTypeInfo.noOutput())
                    .rebalance();
        }

        @Override
        public SimpleVersionedSerializer<Void> getWriteResultSerializer() {
            return null;
        }

        @Override
        public DataStream<Integer> addPreWriteTopology(DataStream<Integer> inputDataStream) {
            return inputDataStream.map(v -> v).name("pre-writer").rebalance();
        }
    }

    private static class TestSinkWithAllInterfaces
            implements Sink<Integer>,
                    SupportsPreWriteTopology<Integer>,
                    SupportsCommitter<Long>,
                    SupportsPreCommitTopology<String, Long>,
                    SupportsPostCommitTopology<Long> {

        @Override
        public CommittingSinkWriter<Integer, String> createWriter(WriterInitContext context)
                throws IOException {
            return new CommittingSinkWriter<Integer, String>() {
                @Override
                public Collection<String> prepareCommit() throws IOException, InterruptedException {
                    return null;
                }

                @Override
                public void write(Integer element, Context context)
                        throws IOException, InterruptedException {}

                @Override
                public void flush(boolean endOfInput) throws IOException, InterruptedException {}

                @Override
                public void close() throws Exception {}
            };
        }

        @Override
        public Committer<Long> createCommitter(CommitterInitContext context) throws IOException {
            return new Committer<Long>() {
                @Override
                public void commit(Collection<CommitRequest<Long>> committables)
                        throws IOException, InterruptedException {}

                @Override
                public void close() throws Exception {}
            };
        }

        @Override
        public SimpleVersionedSerializer<Long> getCommittableSerializer() {
            return new LongSerializer();
        }

        @Override
        public void addPostCommitTopology(DataStream<CommittableMessage<Long>> committables) {
            committables
                    .map(v -> (CommittableMessage<Void>) null)
                    .name("post-committer")
                    .returns(CommittableMessageTypeInfo.noOutput())
                    .rebalance();
        }

        @Override
        public DataStream<CommittableMessage<Long>> addPreCommitTopology(
                DataStream<CommittableMessage<String>> committables) {
            return committables
                    .map(
                            v -> {
                                if (v instanceof CommittableSummary) {
                                    return (CommittableSummary<Long>)
                                            ((CommittableSummary) v).map();
                                } else {
                                    CommittableWithLineage withLineage = (CommittableWithLineage) v;
                                    return (CommittableWithLineage<Long>)
                                            withLineage.map(old -> Long.valueOf(old.toString()));
                                }
                            })
                    .name("pre-committer")
                    .returns(CommittableMessageTypeInfo.of(LongSerializer::new))
                    .rebalance();
        }

        @Override
        public SimpleVersionedSerializer<String> getWriteResultSerializer() {
            return new SimpleVersionedSerializerAdapter<>(StringSerializer.INSTANCE);
        }

        @Override
        public DataStream<Integer> addPreWriteTopology(DataStream<Integer> inputDataStream) {
            return inputDataStream.map(v -> v).name("pre-writer").rebalance();
        }
    }

    public static class LongSerializer implements SimpleVersionedSerializer<Long>, Serializable {
        @Override
        public int getVersion() {
            return 0;
        }

        @Override
        public byte[] serialize(Long obj) throws IOException {
            return new byte[0];
        }

        @Override
        public Long deserialize(int version, byte[] serialized) throws IOException {
            return null;
        }
    }

    private static class SerializationTestOperatorFactory
            extends AbstractStreamOperatorFactory<Integer>
            implements CoordinatedOperatorFactory<Integer> {
        private final boolean isOperatorFactorySerializable;

        SerializationTestOperatorFactory(boolean isOperatorFactorySerializable) {
            this.isOperatorFactorySerializable = isOperatorFactorySerializable;
        }

        @Override
        public OperatorCoordinator.Provider getCoordinatorProvider(
                String operatorName, OperatorID operatorID) {
            return new NonSerializableCoordinatorProvider();
        }

        private void writeObject(ObjectOutputStream oos) throws IOException {
            if (!isOperatorFactorySerializable) {
                throw new IOException("This operator factory is not serializable.");
            }
        }

        @Override
        public <T extends StreamOperator<Integer>> T createStreamOperator(
                StreamOperatorParameters<Integer> parameters) {
            // today's lunch is generics spaghetti
            @SuppressWarnings("unchecked")
            final T castedOperator = (T) new SerializationTestOperator();
            return castedOperator;
        }

        @Override
        public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
            return SerializationTestOperator.class;
        }
    }

    private static class SerializationTestOperator extends AbstractStreamOperator<Integer>
            implements OneInputStreamOperator<Integer, Integer> {

        @Override
        public void processElement(StreamRecord<Integer> element) throws Exception {
            throw new UnsupportedOperationException();
        }
    }

    private static class NonSerializableCoordinatorProvider
            implements OperatorCoordinator.Provider {

        @Override
        public OperatorID getOperatorId() {
            throw new UnsupportedOperationException();
        }

        @Override
        public OperatorCoordinator create(OperatorCoordinator.Context context) throws Exception {
            throw new UnsupportedOperationException();
        }

        private void writeObject(ObjectOutputStream oos) throws IOException {
            throw new IOException("This provider is not serializable.");
        }
    }

    private static JobVertex findJobVertexWithName(List<JobVertex> vertices, String name) {
        for (JobVertex jobVertex : vertices) {
            if (jobVertex.getName().contains(name)) {
                return jobVertex;
            }
        }
        return null;
    }

    private JobGraph createJobGraphWithDescription(
            StreamExecutionEnvironment env, String... inputNames) {
        env.setParallelism(1);
        DataStream<Long> source;
        if (inputNames.length == 1) {
            source = env.fromData(1L, 2L, 3L).setDescription(inputNames[0]);
        } else {
            MultipleInputTransformation<Long> transform =
                    new MultipleInputTransformation<>(
                            "mit", new UnusedOperatorFactory(), Types.LONG, env.getParallelism());
            transform.setDescription("operator chained with source");
            transform.setChainingStrategy(ChainingStrategy.HEAD_WITH_SOURCES);
            Arrays.stream(inputNames)
                    .map(
                            name ->
                                    env.fromSource(
                                                    new NumberSequenceSource(1, 2),
                                                    WatermarkStrategy.noWatermarks(),
                                                    name)
                                            .setDescription(name)
                                            .getTransformation())
                    .forEach(transform::addInput);

            source = new DataStream<>(env, transform);
        }
        DataStream<Long> map1 = source.map(x -> x + 1).setDescription("x + 1");
        DataStream<Long> map2 = source.map(x -> x + 2).setDescription("x + 2");
        map1.print().setDescription("first print of map1");
        map1.print().setDescription("second print of map1");
        map2.print().setDescription("first print of map2");
        map2.print().setDescription("second print of map2");
        return createJobGraph(env.getStreamGraph());
    }

    static final class UnusedOperatorFactory extends AbstractStreamOperatorFactory<Long> {

        @Override
        public <T extends StreamOperator<Long>> T createStreamOperator(
                StreamOperatorParameters<Long> parameters) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
            throw new UnsupportedOperationException();
        }
    }

    public static List<JobVertex> getExpectedVerticesList(List<JobVertex> vertices) {
        final List<JobVertex> verticesMatched = new ArrayList<JobVertex>();
        final List<String> expectedOrder = Arrays.asList("source1", "source2", "map1", "map2");
        for (int i = 0; i < expectedOrder.size(); i++) {
            for (JobVertex vertex : vertices) {
                if (vertex.getName().contains(expectedOrder.get(i))) {
                    verticesMatched.add(vertex);
                }
            }
        }
        return verticesMatched;
    }

    /**
     * Create a StreamGraph as below.
     *
     * <p>source1 --(rebalance & pipelined)--> Map1
     *
     * <p>source2 --(rebalance & blocking)--> Map2
     */
    public static StreamGraph createStreamGraphForSlotSharingTest(Configuration config) {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setBufferTimeout(-1);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        final DataStream<Integer> source1 = env.fromData(1, 2, 3).name("source1");
        source1.rebalance().map(v -> v).name("map1");

        final DataStream<Integer> source2 = env.fromData(4, 5, 6).name("source2");
        final DataStream<Integer> partitioned =
                new DataStream<>(
                        env,
                        new PartitionTransformation<>(
                                source2.getTransformation(),
                                new RebalancePartitioner<>(),
                                StreamExchangeMode.BATCH));
        partitioned.map(v -> v).name("map2");

        return env.getStreamGraph();
    }

    public static void assertSameSlotSharingGroup(JobVertex... vertices) {
        for (int i = 0; i < vertices.length - 1; i++) {
            assertThat(vertices[i + 1].getSlotSharingGroup())
                    .isEqualTo(vertices[i].getSlotSharingGroup());
        }
    }

    public static void assertDistinctSharingGroups(JobVertex... vertices) {
        for (int i = 0; i < vertices.length - 1; i++) {
            for (int j = i + 1; j < vertices.length; j++) {
                assertThat(vertices[i].getSlotSharingGroup())
                        .isNotEqualTo(vertices[j].getSlotSharingGroup());
            }
        }
    }

    public static Method getSetResourcesMethodAndSetAccessible(final Class<?> clazz)
            throws NoSuchMethodException {
        final Method setResourcesMethod =
                clazz.getDeclaredMethod("setResources", ResourceSpec.class);
        setResourcesMethod.setAccessible(true);
        return setResourcesMethod;
    }

    private static class YieldingTestOperatorFactory<T> extends SimpleOperatorFactory<T>
            implements YieldingOperatorFactory<T>, OneInputStreamOperatorFactory<T, T> {
        private YieldingTestOperatorFactory() {
            super(new StreamMap<T, T>(x -> x));
        }

        @Override
        public void setMailboxExecutor(MailboxExecutor mailboxExecutor) {}
    }

    // ------------ private classes -------------
    private static class CoordinatedTransformOperatorFactory
            extends AbstractStreamOperatorFactory<Integer>
            implements CoordinatedOperatorFactory<Integer>,
                    OneInputStreamOperatorFactory<Integer, Integer> {

        @Override
        public OperatorCoordinator.Provider getCoordinatorProvider(
                String operatorName, OperatorID operatorID) {
            return new OperatorCoordinator.Provider() {
                @Override
                public OperatorID getOperatorId() {
                    return null;
                }

                @Override
                public OperatorCoordinator create(OperatorCoordinator.Context context) {
                    return null;
                }
            };
        }

        @Override
        public <T extends StreamOperator<Integer>> T createStreamOperator(
                StreamOperatorParameters<Integer> parameters) {
            return null;
        }

        @Override
        public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
            return null;
        }
    }

    private static class TestingSingleOutputStreamOperator<OUT>
            extends SingleOutputStreamOperator<OUT> {

        public TestingSingleOutputStreamOperator(
                StreamExecutionEnvironment environment, Transformation<OUT> transformation) {
            super(environment, transformation);
        }
    }

    private static class TestingStreamExecutionEnvironment extends StreamExecutionEnvironment {
        Set<AbstractID> completedClusterDatasetIds = new HashSet<>();

        public void addCompletedClusterDataset(AbstractID id) {
            completedClusterDatasetIds.add(id);
        }

        @Override
        public Set<AbstractID> listCompletedClusterDatasets() {
            return new HashSet<>(completedClusterDatasetIds);
        }
    }

    private static class LegacySource implements SourceFunction<Integer> {
        public void run(SourceContext<Integer> sourceContext) {
            sourceContext.collect(1);
        }

        @Override
        public void cancel() {}
    }
}
