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

package org.apache.flink.table.planner.plan.nodes.exec.batch;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.sort.SortCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.runtime.operators.sort.SortOperator;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;

/**
 * {@link BatchExecNode} for Sort without limit.
 *
 * <p>This node will output all data rather than `limit` records.
 */
@ExecNodeMetadata(
        name = "batch-exec-sort",
        version = 1,
        producedTransformations = {BatchExecSort.SORT_TRANSFORMATION},
        consumedOptions = {
            "table.exec.sort.max-num-file-handles",
            "table.exec.sort.async-merge-enabled",
            "table.exec.spill-compression.enabled",
            "table.exec.spill-compression.block-size",
            "table.exec.resource.sort.memory"
        },
        minPlanVersion = FlinkVersion.v2_0,
        minStateVersion = FlinkVersion.v2_0)
public class BatchExecSort extends ExecNodeBase<RowData>
        implements BatchExecNode<RowData>, SingleTransformationTranslator<RowData> {
    public static final String SORT_TRANSFORMATION = "sort";

    public static final String FIELD_NAME_SORT_SPEC = "sortSpec";

    @JsonProperty(FIELD_NAME_SORT_SPEC)
    private final SortSpec sortSpec;

    public BatchExecSort(
            ReadableConfig tableConfig,
            SortSpec sortSpec,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(BatchExecSort.class),
                ExecNodeContext.newPersistedConfig(BatchExecSort.class, tableConfig),
                Collections.singletonList(inputProperty),
                outputType,
                description);
        this.sortSpec = sortSpec;
    }

    @JsonCreator
    public BatchExecSort(
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
            @JsonProperty(FIELD_NAME_SORT_SPEC) SortSpec sortSpec,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        this.sortSpec = sortSpec;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        ExecEdge inputEdge = getInputEdges().get(0);
        Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);

        RowType inputType = (RowType) inputEdge.getOutputType();
        SortCodeGenerator codeGen =
                new SortCodeGenerator(
                        config, planner.getFlinkContext().getClassLoader(), inputType, sortSpec);

        SortOperator operator =
                new SortOperator(
                        codeGen.generateNormalizedKeyComputer("BatchExecSortComputer"),
                        codeGen.generateRecordComparator("BatchExecSortComparator"),
                        config.get(ExecutionConfigOptions.TABLE_EXEC_SORT_MAX_NUM_FILE_HANDLES),
                        config.get(ExecutionConfigOptions.TABLE_EXEC_SPILL_COMPRESSION_ENABLED),
                        (int)
                                config.get(
                                                ExecutionConfigOptions
                                                        .TABLE_EXEC_SPILL_COMPRESSION_BLOCK_SIZE)
                                        .getBytes(),
                        config.get(ExecutionConfigOptions.TABLE_EXEC_SORT_ASYNC_MERGE_ENABLED));
        long sortMemory =
                config.get(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_SORT_MEMORY).getBytes();

        return ExecNodeUtil.createOneInputTransformation(
                inputTransform,
                createTransformationMeta(SORT_TRANSFORMATION, config),
                SimpleOperatorFactory.of(operator),
                InternalTypeInfo.of((RowType) getOutputType()),
                inputTransform.getParallelism(),
                sortMemory,
                false);
    }
}
