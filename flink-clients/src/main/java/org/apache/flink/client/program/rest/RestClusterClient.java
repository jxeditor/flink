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

package org.apache.flink.client.program.rest;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.accumulators.AccumulatorHelper;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.ClientUtils;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.rest.retry.ExponentialWaitStrategy;
import org.apache.flink.client.program.rest.retry.WaitStrategy;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.core.execution.CheckpointType;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.client.JobSubmissionException;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.highavailability.ClientHighAvailabilityServices;
import org.apache.flink.runtime.highavailability.ClientHighAvailabilityServicesFactory;
import org.apache.flink.runtime.highavailability.DefaultClientHighAvailabilityServicesFactory;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobResourceRequirements;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.webmonitor.JobStatusInfo;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.rest.FileUpload;
import org.apache.flink.runtime.rest.HttpHeader;
import org.apache.flink.runtime.rest.RestClient;
import org.apache.flink.runtime.rest.handler.async.AsynchronousOperationInfo;
import org.apache.flink.runtime.rest.handler.async.TriggerResponse;
import org.apache.flink.runtime.rest.handler.legacy.messages.ClusterOverviewWithVersion;
import org.apache.flink.runtime.rest.messages.ClusterOverviewHeaders;
import org.apache.flink.runtime.rest.messages.CustomHeadersDecorator;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.rest.messages.JobAccumulatorsHeaders;
import org.apache.flink.runtime.rest.messages.JobAccumulatorsInfo;
import org.apache.flink.runtime.rest.messages.JobAccumulatorsMessageParameters;
import org.apache.flink.runtime.rest.messages.JobCancellationHeaders;
import org.apache.flink.runtime.rest.messages.JobCancellationMessageParameters;
import org.apache.flink.runtime.rest.messages.JobClientHeartbeatHeaders;
import org.apache.flink.runtime.rest.messages.JobClientHeartbeatParameters;
import org.apache.flink.runtime.rest.messages.JobClientHeartbeatRequestBody;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.JobsOverviewHeaders;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.TerminationModeQueryParameter;
import org.apache.flink.runtime.rest.messages.TriggerId;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointInfo;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointStatusHeaders;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointStatusMessageParameters;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointTriggerHeaders;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointTriggerMessageParameters;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointTriggerRequestBody;
import org.apache.flink.runtime.rest.messages.cluster.ShutdownHeaders;
import org.apache.flink.runtime.rest.messages.dataset.ClusterDataSetDeleteStatusHeaders;
import org.apache.flink.runtime.rest.messages.dataset.ClusterDataSetDeleteStatusMessageParameters;
import org.apache.flink.runtime.rest.messages.dataset.ClusterDataSetDeleteTriggerHeaders;
import org.apache.flink.runtime.rest.messages.dataset.ClusterDataSetDeleteTriggerMessageParameters;
import org.apache.flink.runtime.rest.messages.dataset.ClusterDataSetEntry;
import org.apache.flink.runtime.rest.messages.dataset.ClusterDataSetListHeaders;
import org.apache.flink.runtime.rest.messages.job.JobDetailsHeaders;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;
import org.apache.flink.runtime.rest.messages.job.JobExecutionResultHeaders;
import org.apache.flink.runtime.rest.messages.job.JobResourceRequirementsBody;
import org.apache.flink.runtime.rest.messages.job.JobResourcesRequirementsUpdateHeaders;
import org.apache.flink.runtime.rest.messages.job.JobStatusInfoHeaders;
import org.apache.flink.runtime.rest.messages.job.JobSubmitHeaders;
import org.apache.flink.runtime.rest.messages.job.JobSubmitRequestBody;
import org.apache.flink.runtime.rest.messages.job.JobSubmitResponseBody;
import org.apache.flink.runtime.rest.messages.job.coordination.ClientCoordinationHeaders;
import org.apache.flink.runtime.rest.messages.job.coordination.ClientCoordinationMessageParameters;
import org.apache.flink.runtime.rest.messages.job.coordination.ClientCoordinationRequestBody;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointDisposalRequest;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointDisposalStatusHeaders;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointDisposalStatusMessageParameters;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointDisposalTriggerHeaders;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointInfo;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointStatusHeaders;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointStatusMessageParameters;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerHeaders;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerMessageParameters;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerRequestBody;
import org.apache.flink.runtime.rest.messages.job.savepoints.stop.StopWithSavepointRequestBody;
import org.apache.flink.runtime.rest.messages.job.savepoints.stop.StopWithSavepointTriggerHeaders;
import org.apache.flink.runtime.rest.messages.queue.AsynchronouslyCreatedResource;
import org.apache.flink.runtime.rest.messages.queue.QueueStatus;
import org.apache.flink.runtime.rest.util.RestClientException;
import org.apache.flink.runtime.rest.util.RestConstants;
import org.apache.flink.runtime.webmonitor.retriever.LeaderRetriever;
import org.apache.flink.streaming.api.graph.ExecutionPlan;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.concurrent.FixedRetryStrategy;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.ScheduledExecutorServiceAdapter;
import org.apache.flink.util.function.CheckedSupplier;

import org.apache.flink.shaded.netty4.io.netty.channel.ConnectTimeoutException;
import org.apache.flink.shaded.netty4.io.netty.channel.EventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A {@link ClusterClient} implementation that communicates via HTTP REST requests. */
public class RestClusterClient<T> implements ClusterClient<T> {

    private static final Logger LOG = LoggerFactory.getLogger(RestClusterClient.class);

    private final RestClusterClientConfiguration restClusterClientConfiguration;

    private final Configuration configuration;

    private final RestClient restClient;

    private final ExecutorService executorService =
            Executors.newFixedThreadPool(
                    4, new ExecutorThreadFactory("Flink-RestClusterClient-IO"));

    private final WaitStrategy waitStrategy;

    private final T clusterId;

    private final ClientHighAvailabilityServices clientHAServices;

    private final LeaderRetrievalService webMonitorRetrievalService;

    private final LeaderRetriever webMonitorLeaderRetriever = new LeaderRetriever();

    private final AtomicBoolean running = new AtomicBoolean(true);

    /** ExecutorService to run operations that can be retried on exceptions. */
    private final ScheduledExecutorService retryExecutorService;

    private final Predicate<Throwable> unknownJobStateRetryable =
            exception ->
                    ExceptionUtils.findThrowable(exception, JobStateUnknownException.class)
                            .isPresent();

    private final URL jobmanagerUrl;

    private final Collection<HttpHeader> customHttpHeaders;

    public RestClusterClient(Configuration config, T clusterId) throws Exception {
        this(config, clusterId, DefaultClientHighAvailabilityServicesFactory.INSTANCE);
    }

    public RestClusterClient(
            Configuration config, T clusterId, ClientHighAvailabilityServicesFactory factory)
            throws Exception {
        this(config, null, clusterId, new ExponentialWaitStrategy(10L, 2000L), factory, null);
    }

    public RestClusterClient(
            Configuration config,
            T clusterId,
            ClientHighAvailabilityServicesFactory factory,
            EventLoopGroup group)
            throws Exception {
        this(config, null, clusterId, new ExponentialWaitStrategy(10L, 2000L), factory, group);
    }

    @VisibleForTesting
    RestClusterClient(
            Configuration configuration,
            @Nullable RestClient restClient,
            T clusterId,
            WaitStrategy waitStrategy)
            throws Exception {
        this(
                configuration,
                restClient,
                clusterId,
                waitStrategy,
                DefaultClientHighAvailabilityServicesFactory.INSTANCE,
                null);
    }

    private RestClusterClient(
            Configuration configuration,
            @Nullable RestClient restClient,
            T clusterId,
            WaitStrategy waitStrategy,
            ClientHighAvailabilityServicesFactory clientHAServicesFactory,
            @Nullable EventLoopGroup group)
            throws Exception {
        this.configuration = checkNotNull(configuration);

        this.restClusterClientConfiguration =
                RestClusterClientConfiguration.fromConfiguration(configuration);

        this.customHttpHeaders =
                ClientUtils.readHeadersFromEnvironmentVariable(
                        ConfigConstants.FLINK_REST_CLIENT_HEADERS);
        jobmanagerUrl =
                new URL(
                        SecurityOptions.isRestSSLEnabled(configuration) ? "https" : "http",
                        configuration.get(JobManagerOptions.ADDRESS),
                        configuration.get(JobManagerOptions.PORT),
                        configuration.get(RestOptions.PATH));

        if (restClient != null) {
            this.restClient = restClient;
        } else {
            this.restClient =
                    RestClient.forUrl(configuration, executorService, jobmanagerUrl, group);
        }

        this.waitStrategy = checkNotNull(waitStrategy);
        this.clusterId = checkNotNull(clusterId);

        this.clientHAServices =
                clientHAServicesFactory.create(
                        configuration,
                        exception ->
                                webMonitorLeaderRetriever.handleError(
                                        new FlinkException(
                                                "Fatal error happened with client HA "
                                                        + "services.",
                                                exception)));

        this.webMonitorRetrievalService = clientHAServices.getClusterRestEndpointLeaderRetriever();
        this.retryExecutorService =
                Executors.newSingleThreadScheduledExecutor(
                        new ExecutorThreadFactory("Flink-RestClusterClient-Retry"));
        startLeaderRetrievers();
    }

    private void startLeaderRetrievers() throws Exception {
        this.webMonitorRetrievalService.start(webMonitorLeaderRetriever);
    }

    @Override
    public Configuration getFlinkConfiguration() {
        return new Configuration(configuration);
    }

    @Override
    public void close() {
        if (running.compareAndSet(true, false)) {
            ExecutorUtils.gracefulShutdown(
                    restClusterClientConfiguration.getRetryDelay(),
                    TimeUnit.MILLISECONDS,
                    retryExecutorService);

            this.restClient.shutdown(Duration.ofSeconds(5));
            ExecutorUtils.gracefulShutdown(5, TimeUnit.SECONDS, this.executorService);

            try {
                webMonitorRetrievalService.stop();
            } catch (Exception e) {
                LOG.error("An error occurred during stopping the WebMonitorRetrievalService", e);
            }

            try {
                clientHAServices.close();
            } catch (Exception e) {
                LOG.error(
                        "An error occurred during stopping the ClientHighAvailabilityServices", e);
            }
        }
    }

    /**
     * Requests the job details.
     *
     * @param jobId The job id
     * @return Job details
     */
    public CompletableFuture<JobDetailsInfo> getJobDetails(JobID jobId) {
        final JobDetailsHeaders detailsHeaders = JobDetailsHeaders.getInstance();
        final JobMessageParameters params = new JobMessageParameters();
        params.jobPathParameter.resolve(jobId);

        return sendRequest(detailsHeaders, params);
    }

    @Override
    public CompletableFuture<JobStatus> getJobStatus(JobID jobId) {
        final CheckedSupplier<CompletableFuture<JobStatus>> operation =
                () -> requestJobStatus(jobId);
        return retry(operation, unknownJobStateRetryable);
    }

    /**
     * Requests the {@link JobResult} for the given {@link JobID}. The method retries multiple times
     * to poll the {@link JobResult} before giving up.
     *
     * @param jobId specifying the job for which to retrieve the {@link JobResult}
     * @return Future which is completed with the {@link JobResult} once the job has completed or
     *     with a failure if the {@link JobResult} could not be retrieved.
     */
    @Override
    public CompletableFuture<JobResult> requestJobResult(@Nonnull JobID jobId) {
        final CheckedSupplier<CompletableFuture<JobResult>> operation =
                () -> requestJobResultInternal(jobId);
        return retry(operation, unknownJobStateRetryable);
    }

    @Override
    public CompletableFuture<JobID> submitJob(@Nonnull ExecutionPlan executionPlan) {
        CompletableFuture<java.nio.file.Path> executionPlanFileFuture =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                final java.nio.file.Path executionPlanFile =
                                        Files.createTempFile(
                                                "flink-executionPlan-" + executionPlan.getJobID(),
                                                ".bin");
                                try (ObjectOutputStream objectOut =
                                        new ObjectOutputStream(
                                                Files.newOutputStream(executionPlanFile))) {
                                    objectOut.writeObject(executionPlan);
                                }
                                return executionPlanFile;
                            } catch (IOException e) {
                                throw new CompletionException(
                                        new FlinkException(
                                                "Failed to serialize ExecutionPlan.", e));
                            }
                        },
                        executorService);

        CompletableFuture<Tuple2<JobSubmitRequestBody, Collection<FileUpload>>> requestFuture =
                executionPlanFileFuture.thenApply(
                        executionPlanFile -> {
                            List<String> jarFileNames = new ArrayList<>(8);
                            List<JobSubmitRequestBody.DistributedCacheFile> artifactFileNames =
                                    new ArrayList<>(8);
                            Collection<FileUpload> filesToUpload = new ArrayList<>(8);

                            filesToUpload.add(
                                    new FileUpload(
                                            executionPlanFile, RestConstants.CONTENT_TYPE_BINARY));

                            for (Path jar : executionPlan.getUserJars()) {
                                jarFileNames.add(jar.getName());
                                filesToUpload.add(
                                        new FileUpload(
                                                Paths.get(jar.toUri()),
                                                RestConstants.CONTENT_TYPE_JAR));
                            }

                            for (Map.Entry<String, DistributedCache.DistributedCacheEntry>
                                    artifacts : executionPlan.getUserArtifacts().entrySet()) {
                                final Path artifactFilePath =
                                        new Path(artifacts.getValue().filePath);
                                try {
                                    // Only local artifacts need to be uploaded.
                                    if (!artifactFilePath.getFileSystem().isDistributedFS()) {
                                        artifactFileNames.add(
                                                new JobSubmitRequestBody.DistributedCacheFile(
                                                        artifacts.getKey(),
                                                        artifactFilePath.getName()));
                                        filesToUpload.add(
                                                new FileUpload(
                                                        Paths.get(artifactFilePath.getPath()),
                                                        RestConstants.CONTENT_TYPE_BINARY));
                                    }
                                } catch (IOException e) {
                                    throw new CompletionException(
                                            new FlinkException(
                                                    "Failed to get the FileSystem of artifact "
                                                            + artifactFilePath
                                                            + ".",
                                                    e));
                                }
                            }

                            final JobSubmitRequestBody requestBody =
                                    new JobSubmitRequestBody(
                                            executionPlanFile.getFileName().toString(),
                                            jarFileNames,
                                            artifactFileNames);

                            return Tuple2.of(
                                    requestBody, Collections.unmodifiableCollection(filesToUpload));
                        });

        final CompletableFuture<JobSubmitResponseBody> submissionFuture =
                requestFuture.thenCompose(
                        requestAndFileUploads -> {
                            LOG.info(
                                    "Submitting job '{}' ({}).",
                                    executionPlan.getName(),
                                    executionPlan.getJobID());
                            return sendRetriableRequest(
                                    JobSubmitHeaders.getInstance(),
                                    EmptyMessageParameters.getInstance(),
                                    requestAndFileUploads.f0,
                                    requestAndFileUploads.f1,
                                    isConnectionProblemOrServiceUnavailable(),
                                    (receiver, error) -> {
                                        if (error != null) {
                                            LOG.warn(
                                                    "Attempt to submit job '{}' ({}) to '{}' has failed.",
                                                    executionPlan.getName(),
                                                    executionPlan.getJobID(),
                                                    receiver,
                                                    error);
                                        } else {
                                            LOG.info(
                                                    "Successfully submitted job '{}' ({}) to '{}'.",
                                                    executionPlan.getName(),
                                                    executionPlan.getJobID(),
                                                    receiver);
                                        }
                                    });
                        });

        submissionFuture
                .exceptionally(ignored -> null) // ignore errors
                .thenCompose(ignored -> executionPlanFileFuture)
                .thenAccept(
                        executionPlanFile -> {
                            try {
                                Files.delete(executionPlanFile);
                            } catch (IOException e) {
                                LOG.warn(
                                        "Could not delete temporary file {}.",
                                        executionPlanFile,
                                        e);
                            }
                        });

        return submissionFuture
                .thenApply(ignore -> executionPlan.getJobID())
                .exceptionally(
                        (Throwable throwable) -> {
                            throw new CompletionException(
                                    new JobSubmissionException(
                                            executionPlan.getJobID(),
                                            "Failed to submit ExecutionPlan.",
                                            ExceptionUtils.stripCompletionException(throwable)));
                        });
    }

    @Override
    public CompletableFuture<Acknowledge> cancel(JobID jobID) {
        JobCancellationMessageParameters params =
                new JobCancellationMessageParameters()
                        .resolveJobId(jobID)
                        .resolveTerminationMode(
                                TerminationModeQueryParameter.TerminationMode.CANCEL);
        CompletableFuture<EmptyResponseBody> responseFuture =
                sendRequest(JobCancellationHeaders.getInstance(), params);
        return responseFuture.thenApply(ignore -> Acknowledge.get());
    }

    @Override
    public CompletableFuture<String> stopWithSavepoint(
            final JobID jobId,
            final boolean advanceToEndOfTime,
            @Nullable final String savepointDirectory,
            final SavepointFormatType formatType) {
        return stopWithSavepoint(jobId, advanceToEndOfTime, savepointDirectory, formatType, false);
    }

    @Override
    public CompletableFuture<String> stopWithDetachedSavepoint(
            final JobID jobId,
            final boolean advanceToEndOfTime,
            @Nullable final String savepointDirectory,
            final SavepointFormatType formatType) {
        return stopWithSavepoint(jobId, advanceToEndOfTime, savepointDirectory, formatType, true);
    }

    @Override
    public CompletableFuture<String> cancelWithSavepoint(
            JobID jobId, @Nullable String savepointDirectory, SavepointFormatType formatType) {
        return triggerSavepoint(jobId, savepointDirectory, true, formatType, false);
    }

    @Override
    public CompletableFuture<String> triggerSavepoint(
            final JobID jobId,
            final @Nullable String savepointDirectory,
            final SavepointFormatType formatType) {
        return triggerSavepoint(jobId, savepointDirectory, false, formatType, false);
    }

    @Override
    public CompletableFuture<Long> triggerCheckpoint(JobID jobId, CheckpointType checkpointType) {
        final CheckpointTriggerHeaders checkpointTriggerHeaders =
                CheckpointTriggerHeaders.getInstance();
        final CheckpointTriggerMessageParameters checkpointTriggerMessageParameters =
                checkpointTriggerHeaders.getUnresolvedMessageParameters();
        checkpointTriggerMessageParameters.jobID.resolve(jobId);

        final CompletableFuture<TriggerResponse> responseFuture =
                sendRequest(
                        checkpointTriggerHeaders,
                        checkpointTriggerMessageParameters,
                        new CheckpointTriggerRequestBody(checkpointType, null));

        return responseFuture
                .thenCompose(
                        checkpointTriggerResponseBody -> {
                            final TriggerId checkpointTriggerId =
                                    checkpointTriggerResponseBody.getTriggerId();
                            return pollCheckpointAsync(jobId, checkpointTriggerId);
                        })
                .thenApply(
                        checkpointInfo -> {
                            if (checkpointInfo.getFailureCause() != null) {
                                throw new CompletionException(checkpointInfo.getFailureCause());
                            }
                            return checkpointInfo.getCheckpointId();
                        });
    }

    @Override
    public CompletableFuture<String> triggerDetachedSavepoint(
            final JobID jobId,
            final @Nullable String savepointDirectory,
            final SavepointFormatType formatType) {
        return triggerSavepoint(jobId, savepointDirectory, false, formatType, true);
    }

    @Override
    public CompletableFuture<CoordinationResponse> sendCoordinationRequest(
            JobID jobId, String operatorUid, CoordinationRequest request) {
        ClientCoordinationHeaders headers = ClientCoordinationHeaders.getInstance();
        ClientCoordinationMessageParameters params = new ClientCoordinationMessageParameters();
        params.jobPathParameter.resolve(jobId);
        params.operatorUidPathParameter.resolve(operatorUid);

        SerializedValue<CoordinationRequest> serializedRequest;
        try {
            serializedRequest = new SerializedValue<>(request);
        } catch (IOException e) {
            return FutureUtils.completedExceptionally(e);
        }

        ClientCoordinationRequestBody requestBody =
                new ClientCoordinationRequestBody(serializedRequest);
        return sendRequest(headers, params, requestBody)
                .thenApply(
                        responseBody -> {
                            try {
                                return responseBody
                                        .getSerializedCoordinationResponse()
                                        .deserializeValue(getClass().getClassLoader());
                            } catch (IOException | ClassNotFoundException e) {
                                throw new CompletionException(
                                        "Failed to deserialize coordination response", e);
                            }
                        });
    }

    public CompletableFuture<String> stopWithSavepoint(
            final JobID jobId,
            final boolean advanceToEndOfTime,
            @Nullable final String savepointDirectory,
            final SavepointFormatType formatType,
            final boolean isDetachedMode) {

        final StopWithSavepointTriggerHeaders stopWithSavepointTriggerHeaders =
                StopWithSavepointTriggerHeaders.getInstance();

        final SavepointTriggerMessageParameters stopWithSavepointTriggerMessageParameters =
                stopWithSavepointTriggerHeaders.getUnresolvedMessageParameters();
        stopWithSavepointTriggerMessageParameters.jobID.resolve(jobId);

        final CompletableFuture<TriggerResponse> responseFuture =
                sendRequest(
                        stopWithSavepointTriggerHeaders,
                        stopWithSavepointTriggerMessageParameters,
                        new StopWithSavepointRequestBody(
                                savepointDirectory, advanceToEndOfTime, formatType, null));

        return getSavepointTriggerFuture(jobId, isDetachedMode, responseFuture);
    }

    private CompletableFuture<String> triggerSavepoint(
            final JobID jobId,
            final @Nullable String savepointDirectory,
            final boolean cancelJob,
            final SavepointFormatType formatType,
            final boolean isDetachedMode) {
        final SavepointTriggerHeaders savepointTriggerHeaders =
                SavepointTriggerHeaders.getInstance();
        final SavepointTriggerMessageParameters savepointTriggerMessageParameters =
                savepointTriggerHeaders.getUnresolvedMessageParameters();
        savepointTriggerMessageParameters.jobID.resolve(jobId);

        final CompletableFuture<TriggerResponse> responseFuture =
                sendRequest(
                        savepointTriggerHeaders,
                        savepointTriggerMessageParameters,
                        new SavepointTriggerRequestBody(
                                savepointDirectory, cancelJob, formatType, null));

        return getSavepointTriggerFuture(jobId, isDetachedMode, responseFuture);
    }

    private CompletableFuture<String> getSavepointTriggerFuture(
            JobID jobId,
            boolean isDetachedMode,
            CompletableFuture<TriggerResponse> responseFuture) {
        CompletableFuture<String> futureResult;
        if (isDetachedMode) {
            // we just return the savepoint trigger id in detached savepoint,
            // that means the client could exit immediately
            futureResult =
                    responseFuture.thenApply((TriggerResponse tr) -> tr.getTriggerId().toString());
        } else {
            // otherwise we need to wait the savepoint to be succeeded
            // and return the savepoint path
            futureResult =
                    responseFuture
                            .thenCompose(
                                    savepointTriggerResponseBody -> {
                                        final TriggerId savepointTriggerId =
                                                savepointTriggerResponseBody.getTriggerId();
                                        return pollSavepointAsync(jobId, savepointTriggerId);
                                    })
                            .thenApply(
                                    savepointInfo -> {
                                        if (savepointInfo.getFailureCause() != null) {
                                            throw new CompletionException(
                                                    savepointInfo.getFailureCause());
                                        }
                                        return savepointInfo.getLocation();
                                    });
        }

        return futureResult;
    }

    @Override
    public CompletableFuture<Map<String, Object>> getAccumulators(JobID jobID, ClassLoader loader) {
        final JobAccumulatorsHeaders accumulatorsHeaders = JobAccumulatorsHeaders.getInstance();
        final JobAccumulatorsMessageParameters accMsgParams =
                accumulatorsHeaders.getUnresolvedMessageParameters();
        accMsgParams.jobPathParameter.resolve(jobID);
        accMsgParams.includeSerializedAccumulatorsParameter.resolve(
                Collections.singletonList(true));

        CompletableFuture<JobAccumulatorsInfo> responseFuture =
                sendRequest(accumulatorsHeaders, accMsgParams);

        return responseFuture
                .thenApply(JobAccumulatorsInfo::getSerializedUserAccumulators)
                .thenApply(
                        accumulators -> {
                            try {
                                return AccumulatorHelper.deserializeAndUnwrapAccumulators(
                                        accumulators, loader);
                            } catch (Exception e) {
                                throw new CompletionException(
                                        "Cannot deserialize and unwrap accumulators properly.", e);
                            }
                        });
    }

    private CompletableFuture<SavepointInfo> pollSavepointAsync(
            final JobID jobId, final TriggerId triggerID) {
        return pollResourceAsync(
                () -> {
                    final SavepointStatusHeaders savepointStatusHeaders =
                            SavepointStatusHeaders.getInstance();
                    final SavepointStatusMessageParameters savepointStatusMessageParameters =
                            savepointStatusHeaders.getUnresolvedMessageParameters();
                    savepointStatusMessageParameters.jobIdPathParameter.resolve(jobId);
                    savepointStatusMessageParameters.triggerIdPathParameter.resolve(triggerID);
                    return sendRequest(savepointStatusHeaders, savepointStatusMessageParameters);
                });
    }

    private CompletableFuture<CheckpointInfo> pollCheckpointAsync(
            final JobID jobId, final TriggerId triggerID) {
        return pollResourceAsync(
                () -> {
                    final CheckpointStatusHeaders checkpointStatusHeaders =
                            CheckpointStatusHeaders.getInstance();
                    final CheckpointStatusMessageParameters checkpointStatusMessageParameters =
                            checkpointStatusHeaders.getUnresolvedMessageParameters();
                    checkpointStatusMessageParameters.jobIdPathParameter.resolve(jobId);
                    checkpointStatusMessageParameters.triggerIdPathParameter.resolve(triggerID);
                    return sendRequest(checkpointStatusHeaders, checkpointStatusMessageParameters);
                });
    }

    @Override
    public CompletableFuture<Collection<JobStatusMessage>> listJobs() {
        return sendRequest(JobsOverviewHeaders.getInstance())
                .thenApply(
                        (multipleJobsDetails) ->
                                multipleJobsDetails.getJobs().stream()
                                        .map(
                                                detail ->
                                                        new JobStatusMessage(
                                                                detail.getJobId(),
                                                                detail.getJobName(),
                                                                detail.getStatus(),
                                                                detail.getStartTime()))
                                        .collect(Collectors.toList()));
    }

    @Override
    public T getClusterId() {
        return clusterId;
    }

    @Override
    public CompletableFuture<Acknowledge> disposeSavepoint(String savepointPath) {
        final SavepointDisposalRequest savepointDisposalRequest =
                new SavepointDisposalRequest(savepointPath);

        final CompletableFuture<TriggerResponse> savepointDisposalTriggerFuture =
                sendRequest(
                        SavepointDisposalTriggerHeaders.getInstance(), savepointDisposalRequest);

        final CompletableFuture<AsynchronousOperationInfo> savepointDisposalFuture =
                savepointDisposalTriggerFuture.thenCompose(
                        (TriggerResponse triggerResponse) -> {
                            final TriggerId triggerId = triggerResponse.getTriggerId();
                            final SavepointDisposalStatusHeaders savepointDisposalStatusHeaders =
                                    SavepointDisposalStatusHeaders.getInstance();
                            final SavepointDisposalStatusMessageParameters
                                    savepointDisposalStatusMessageParameters =
                                            savepointDisposalStatusHeaders
                                                    .getUnresolvedMessageParameters();
                            savepointDisposalStatusMessageParameters.triggerIdPathParameter.resolve(
                                    triggerId);

                            return pollResourceAsync(
                                    () ->
                                            sendRequest(
                                                    savepointDisposalStatusHeaders,
                                                    savepointDisposalStatusMessageParameters));
                        });

        return savepointDisposalFuture.thenApply(
                (AsynchronousOperationInfo asynchronousOperationInfo) -> {
                    if (asynchronousOperationInfo.getFailureCause() == null) {
                        return Acknowledge.get();
                    } else {
                        throw new CompletionException(asynchronousOperationInfo.getFailureCause());
                    }
                });
    }

    @Override
    public CompletableFuture<Set<AbstractID>> listCompletedClusterDatasetIds() {
        return sendRequest(ClusterDataSetListHeaders.INSTANCE)
                .thenApply(
                        clusterDataSetListResponseBody ->
                                clusterDataSetListResponseBody.getDataSets().stream()
                                        .filter(ClusterDataSetEntry::isComplete)
                                        .map(ClusterDataSetEntry::getDataSetId)
                                        .map(id -> new AbstractID(StringUtils.hexStringToByte(id)))
                                        .collect(Collectors.toSet()));
    }

    @Override
    public CompletableFuture<Void> invalidateClusterDataset(AbstractID clusterDatasetId) {

        final ClusterDataSetDeleteTriggerHeaders triggerHeader =
                ClusterDataSetDeleteTriggerHeaders.INSTANCE;
        final ClusterDataSetDeleteTriggerMessageParameters parameters =
                triggerHeader.getUnresolvedMessageParameters();
        parameters.clusterDataSetIdPathParameter.resolve(
                new IntermediateDataSetID(clusterDatasetId));

        final CompletableFuture<TriggerResponse> triggerFuture =
                sendRequest(triggerHeader, parameters);

        final CompletableFuture<AsynchronousOperationInfo> clusterDatasetDeleteFuture =
                triggerFuture.thenCompose(
                        triggerResponse -> {
                            final TriggerId triggerId = triggerResponse.getTriggerId();
                            final ClusterDataSetDeleteStatusHeaders statusHeaders =
                                    ClusterDataSetDeleteStatusHeaders.INSTANCE;
                            final ClusterDataSetDeleteStatusMessageParameters
                                    statusMessageParameters =
                                            statusHeaders.getUnresolvedMessageParameters();
                            statusMessageParameters.triggerIdPathParameter.resolve(triggerId);

                            return pollResourceAsync(
                                    () -> sendRequest(statusHeaders, statusMessageParameters));
                        });

        return clusterDatasetDeleteFuture.thenApply(
                asynchronousOperationInfo -> {
                    if (asynchronousOperationInfo.getFailureCause() == null) {
                        return null;
                    } else {
                        throw new CompletionException(asynchronousOperationInfo.getFailureCause());
                    }
                });
    }

    @Override
    public CompletableFuture<Void> reportHeartbeat(JobID jobId, long expiredTimestamp) {
        JobClientHeartbeatParameters params =
                new JobClientHeartbeatParameters().resolveJobId(jobId);
        CompletableFuture<EmptyResponseBody> responseFuture =
                sendRequest(
                        JobClientHeartbeatHeaders.getInstance(),
                        params,
                        new JobClientHeartbeatRequestBody(expiredTimestamp));
        return responseFuture.thenApply(ignore -> null);
    }

    @Override
    public void shutDownCluster() {
        try {
            sendRequest(ShutdownHeaders.getInstance()).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            LOG.error("Error while shutting down cluster", e);
        }
    }

    /**
     * Creates a {@code CompletableFuture} that polls a {@code AsynchronouslyCreatedResource} until
     * its {@link AsynchronouslyCreatedResource#queueStatus() QueueStatus} becomes {@link
     * QueueStatus.Id#COMPLETED COMPLETED}. The future completes with the result of {@link
     * AsynchronouslyCreatedResource#resource()}.
     *
     * @param resourceFutureSupplier The operation which polls for the {@code
     *     AsynchronouslyCreatedResource}.
     * @param <R> The type of the resource.
     * @param <A> The type of the {@code AsynchronouslyCreatedResource}.
     * @return A {@code CompletableFuture} delivering the resource.
     */
    private <R, A extends AsynchronouslyCreatedResource<R>> CompletableFuture<R> pollResourceAsync(
            final Supplier<CompletableFuture<A>> resourceFutureSupplier) {
        return pollResourceAsync(resourceFutureSupplier, new CompletableFuture<>(), 0);
    }

    private <R, A extends AsynchronouslyCreatedResource<R>> CompletableFuture<R> pollResourceAsync(
            final Supplier<CompletableFuture<A>> resourceFutureSupplier,
            final CompletableFuture<R> resultFuture,
            final long attempt) {

        resourceFutureSupplier
                .get()
                .whenComplete(
                        (asynchronouslyCreatedResource, throwable) -> {
                            if (throwable != null) {
                                resultFuture.completeExceptionally(throwable);
                            } else {
                                if (asynchronouslyCreatedResource.queueStatus().getId()
                                        == QueueStatus.Id.COMPLETED) {
                                    resultFuture.complete(asynchronouslyCreatedResource.resource());
                                } else {
                                    retryExecutorService.schedule(
                                            () -> {
                                                pollResourceAsync(
                                                        resourceFutureSupplier,
                                                        resultFuture,
                                                        attempt + 1);
                                            },
                                            waitStrategy.sleepTime(attempt),
                                            TimeUnit.MILLISECONDS);
                                }
                            }
                        });

        return resultFuture;
    }

    /**
     * Update {@link JobResourceRequirements} of a given job.
     *
     * @param jobId jobId specifies the job for which to change the resource requirements
     * @param jobResourceRequirements new resource requirements for the provided job
     * @return Future which is completed upon successful operation.
     */
    public CompletableFuture<Acknowledge> updateJobResourceRequirements(
            JobID jobId, JobResourceRequirements jobResourceRequirements) {
        final JobMessageParameters params = new JobMessageParameters();
        params.jobPathParameter.resolve(jobId);

        return sendRequest(
                        JobResourcesRequirementsUpdateHeaders.INSTANCE,
                        params,
                        new JobResourceRequirementsBody(jobResourceRequirements))
                .thenApply(ignored -> Acknowledge.get());
    }

    @VisibleForTesting
    URL getJobmanagerUrl() {
        return jobmanagerUrl;
    }

    @VisibleForTesting
    Collection<HttpHeader> getCustomHttpHeaders() {
        return customHttpHeaders;
    }

    /**
     * Get an overview of the Flink cluster.
     *
     * @return Future with the {@link ClusterOverviewWithVersion cluster overview}.
     */
    public CompletableFuture<ClusterOverviewWithVersion> getClusterOverview() {
        return sendRequest(
                ClusterOverviewHeaders.getInstance(),
                EmptyMessageParameters.getInstance(),
                EmptyRequestBody.getInstance());
    }

    // ======================================
    // Legacy stuff we actually implement
    // ======================================

    @Override
    public String getWebInterfaceURL() {
        try {
            return getWebMonitorBaseUrl().get().toString();
        } catch (InterruptedException | ExecutionException e) {
            ExceptionUtils.checkInterrupted(e);

            LOG.warn("Could not retrieve the web interface URL for the cluster.", e);
            return "Unknown address.";
        }
    }

    // -------------------------------------------------------------------------
    // RestClient Helper
    // -------------------------------------------------------------------------

    private CompletableFuture<JobStatus> requestJobStatus(JobID jobId) {
        final JobStatusInfoHeaders jobStatusInfoHeaders = JobStatusInfoHeaders.getInstance();
        final JobMessageParameters params = new JobMessageParameters();
        params.jobPathParameter.resolve(jobId);

        return sendRequest(jobStatusInfoHeaders, params)
                .thenApply(JobStatusInfo::getJobStatus)
                .thenApply(
                        jobStatus -> {
                            if (jobStatus == JobStatus.SUSPENDED) {
                                throw new JobStateUnknownException(
                                        String.format("Job %s is in state SUSPENDED", jobId));
                            }
                            return jobStatus;
                        });
    }

    private static class JobStateUnknownException extends RuntimeException {
        public JobStateUnknownException(String message) {
            super(message);
        }
    }

    private CompletableFuture<JobResult> requestJobResultInternal(@Nonnull JobID jobId) {
        return pollResourceAsync(
                        () -> {
                            final JobMessageParameters messageParameters =
                                    new JobMessageParameters();
                            messageParameters.jobPathParameter.resolve(jobId);
                            return sendRequest(
                                    JobExecutionResultHeaders.getInstance(), messageParameters);
                        })
                .thenApply(
                        jobResult -> {
                            if (jobResult.getApplicationStatus() == ApplicationStatus.UNKNOWN) {
                                throw new JobStateUnknownException(
                                        String.format("Result for Job %s is UNKNOWN", jobId));
                            }
                            return jobResult;
                        });
    }

    private <
                    M extends MessageHeaders<EmptyRequestBody, P, U>,
                    U extends MessageParameters,
                    P extends ResponseBody>
            CompletableFuture<P> sendRequest(M messageHeaders, U messageParameters) {
        return sendRequest(messageHeaders, messageParameters, EmptyRequestBody.getInstance());
    }

    private <
                    M extends MessageHeaders<R, P, EmptyMessageParameters>,
                    R extends RequestBody,
                    P extends ResponseBody>
            CompletableFuture<P> sendRequest(M messageHeaders, R request) {
        return sendRequest(messageHeaders, EmptyMessageParameters.getInstance(), request);
    }

    @VisibleForTesting
    <M extends MessageHeaders<EmptyRequestBody, P, EmptyMessageParameters>, P extends ResponseBody>
            CompletableFuture<P> sendRequest(M messageHeaders) {
        return sendRequest(
                messageHeaders,
                EmptyMessageParameters.getInstance(),
                EmptyRequestBody.getInstance());
    }

    @VisibleForTesting
    public <
                    M extends MessageHeaders<R, P, U>,
                    U extends MessageParameters,
                    R extends RequestBody,
                    P extends ResponseBody>
            CompletableFuture<P> sendRequest(M messageHeaders, U messageParameters, R request) {
        return sendRetriableRequest(
                messageHeaders,
                messageParameters,
                request,
                isConnectionProblemOrServiceUnavailable());
    }

    private <
                    M extends MessageHeaders<R, P, U>,
                    U extends MessageParameters,
                    R extends RequestBody,
                    P extends ResponseBody>
            CompletableFuture<P> sendRetriableRequest(
                    M messageHeaders,
                    U messageParameters,
                    R request,
                    Predicate<Throwable> retryPredicate) {
        return sendRetriableRequest(
                messageHeaders,
                messageParameters,
                request,
                Collections.emptyList(),
                retryPredicate,
                (receiver, error) -> {
                    // no-op
                });
    }

    private <
                    M extends MessageHeaders<R, P, U>,
                    U extends MessageParameters,
                    R extends RequestBody,
                    P extends ResponseBody>
            CompletableFuture<P> sendRetriableRequest(
                    M messageHeaders,
                    U messageParameters,
                    R request,
                    Collection<FileUpload> filesToUpload,
                    Predicate<Throwable> retryPredicate,
                    BiConsumer<String, Throwable> consumer) {
        return retry(
                () ->
                        getWebMonitorBaseUrl()
                                .thenCompose(
                                        webMonitorBaseUrl -> {
                                            try {
                                                CustomHeadersDecorator<R, P, U> headers =
                                                        new CustomHeadersDecorator<>(
                                                                new UrlPrefixDecorator<>(
                                                                        messageHeaders,
                                                                        jobmanagerUrl.getPath()));
                                                headers.setCustomHeaders(customHttpHeaders);
                                                final CompletableFuture<P> future =
                                                        restClient.sendRequest(
                                                                webMonitorBaseUrl.getHost(),
                                                                webMonitorBaseUrl.getPort(),
                                                                headers,
                                                                messageParameters,
                                                                request,
                                                                filesToUpload);
                                                future.whenComplete(
                                                        (result, error) ->
                                                                consumer.accept(
                                                                        webMonitorBaseUrl
                                                                                .toString(),
                                                                        error));
                                                return future;
                                            } catch (IOException e) {
                                                throw new CompletionException(e);
                                            }
                                        }),
                retryPredicate);
    }

    private <C> CompletableFuture<C> retry(
            CheckedSupplier<CompletableFuture<C>> operation, Predicate<Throwable> retryPredicate) {
        return FutureUtils.retryWithDelay(
                CheckedSupplier.unchecked(operation),
                new FixedRetryStrategy(
                        restClusterClientConfiguration.getRetryMaxAttempts(),
                        Duration.ofMillis(restClusterClientConfiguration.getRetryDelay())),
                retryPredicate,
                new ScheduledExecutorServiceAdapter(retryExecutorService));
    }

    private static Predicate<Throwable> isConnectionProblemOrServiceUnavailable() {
        return isConnectionProblemException().or(isServiceUnavailable());
    }

    private static Predicate<Throwable> isConnectionProblemException() {
        return (throwable) ->
                ExceptionUtils.findThrowable(throwable, java.net.ConnectException.class).isPresent()
                        || ExceptionUtils.findThrowable(
                                        throwable, java.net.SocketTimeoutException.class)
                                .isPresent()
                        || ExceptionUtils.findThrowable(throwable, ConnectTimeoutException.class)
                                .isPresent()
                        || ExceptionUtils.findThrowable(throwable, IOException.class).isPresent();
    }

    private static Predicate<Throwable> isServiceUnavailable() {
        return httpExceptionCodePredicate(
                code -> code == HttpResponseStatus.SERVICE_UNAVAILABLE.code());
    }

    private static Predicate<Throwable> httpExceptionCodePredicate(
            Predicate<Integer> statusCodePredicate) {
        return (throwable) ->
                ExceptionUtils.findThrowable(throwable, RestClientException.class)
                        .map(
                                restClientException -> {
                                    final int code =
                                            restClientException.getHttpResponseStatus().code();
                                    return statusCodePredicate.test(code);
                                })
                        .orElse(false);
    }

    @VisibleForTesting
    CompletableFuture<URL> getWebMonitorBaseUrl() {
        return FutureUtils.orTimeout(
                        webMonitorLeaderRetriever.getLeaderFuture(),
                        restClusterClientConfiguration.getAwaitLeaderTimeout(),
                        TimeUnit.MILLISECONDS,
                        String.format(
                                "Waiting for leader address of WebMonitorEndpoint timed out after %d ms.",
                                restClusterClientConfiguration.getAwaitLeaderTimeout()))
                .thenApplyAsync(
                        leaderAddressSessionId -> {
                            final String url = leaderAddressSessionId.f0;
                            try {
                                return new URL(url);
                            } catch (MalformedURLException e) {
                                throw new IllegalArgumentException(
                                        "Could not parse URL from " + url, e);
                            }
                        },
                        executorService);
    }
}
