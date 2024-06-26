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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionIndexSet;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageInputChannelId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.AvailabilityNotifier;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierConsumerAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierShuffleDescriptor;

import java.io.IOException;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;

/** Test implementation for {@link TierConsumerAgent}. */
public class TestingTierConsumerAgent implements TierConsumerAgent {

    private final Consumer<TieredStorageMemoryManager> memoryManagerConsumer;

    private final Runnable startNotifier;

    private final Supplier<Buffer> bufferSupplier;

    private final Runnable availabilityNotifierRegistrationRunnable;

    private final Runnable updateTierShuffleDescriptorRunnable;

    private final Runnable closeNotifier;

    private final BiFunction<TieredStoragePartitionId, ResultSubpartitionIndexSet, Integer>
            peekNextBufferSubpartitionIdFunction;

    private TestingTierConsumerAgent(
            Runnable startNotifier,
            Consumer<TieredStorageMemoryManager> memoryManagerConsumer,
            Supplier<Buffer> bufferSupplier,
            Runnable availabilityNotifierRegistrationRunnable,
            Runnable updateTierShuffleDescriptorRunnable,
            Runnable closeNotifier,
            BiFunction<TieredStoragePartitionId, ResultSubpartitionIndexSet, Integer>
                    peekNextBufferSubpartitionIdFunction) {
        this.startNotifier = startNotifier;
        this.memoryManagerConsumer = memoryManagerConsumer;
        this.bufferSupplier = bufferSupplier;
        this.availabilityNotifierRegistrationRunnable = availabilityNotifierRegistrationRunnable;
        this.updateTierShuffleDescriptorRunnable = updateTierShuffleDescriptorRunnable;
        this.closeNotifier = closeNotifier;
        this.peekNextBufferSubpartitionIdFunction = peekNextBufferSubpartitionIdFunction;
    }

    @Override
    public void setup(TieredStorageMemoryManager memoryManager) {
        memoryManagerConsumer.accept(memoryManager);
    }

    @Override
    public void start() {
        startNotifier.run();
    }

    @Override
    public int peekNextBufferSubpartitionId(
            TieredStoragePartitionId partitionId, ResultSubpartitionIndexSet indexSet)
            throws IOException {
        return peekNextBufferSubpartitionIdFunction.apply(partitionId, indexSet);
    }

    @Override
    public Optional<Buffer> getNextBuffer(
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId,
            int segmentId) {
        Buffer buffer = bufferSupplier.get();
        return buffer == null ? Optional.empty() : Optional.of(buffer);
    }

    @Override
    public void registerAvailabilityNotifier(AvailabilityNotifier notifier) {
        availabilityNotifierRegistrationRunnable.run();
    }

    @Override
    public void updateTierShuffleDescriptor(
            TieredStoragePartitionId partitionId,
            TieredStorageInputChannelId inputChannelId,
            TieredStorageSubpartitionId subpartitionId,
            TierShuffleDescriptor tierShuffleDescriptor) {
        updateTierShuffleDescriptorRunnable.run();
    }

    @Override
    public void close() throws IOException {
        closeNotifier.run();
    }

    /** Builder for {@link TestingTierConsumerAgent}. */
    public static class Builder {

        private Runnable startNotifier = () -> {};

        private Consumer<TieredStorageMemoryManager> memoryManagerConsumer = memoryManager -> {};

        private Supplier<Buffer> bufferSupplier = () -> null;

        private Runnable availabilityNotifierRegistrationRunnable = () -> {};

        private Runnable updateTierShuffleDescriptorRunnable = () -> {};

        private Runnable closeNotifier = () -> {};

        private BiFunction<TieredStoragePartitionId, ResultSubpartitionIndexSet, Integer>
                peekNextBufferSubpartitionIdFunction = (ignore1, ignore2) -> -1;

        public Builder() {}

        public Builder setStartNotifier(Runnable startNotifier) {
            this.startNotifier = startNotifier;
            return this;
        }

        public Builder setMemoryManagerConsumer(
                Consumer<TieredStorageMemoryManager> memoryManagerConsumer) {
            this.memoryManagerConsumer = memoryManagerConsumer;
            return this;
        }

        public Builder setBufferSupplier(Supplier<Buffer> bufferSupplier) {
            this.bufferSupplier = bufferSupplier;
            return this;
        }

        public Builder setAvailabilityNotifierRegistrationRunnable(
                Runnable availabilityNotifierRegistrationRunnable) {
            this.availabilityNotifierRegistrationRunnable =
                    availabilityNotifierRegistrationRunnable;
            return this;
        }

        public Builder setUpdateTierShuffleDescriptorRunnable(
                Runnable updateTierShuffleDescriptorRunnable) {
            this.updateTierShuffleDescriptorRunnable = updateTierShuffleDescriptorRunnable;
            return this;
        }

        public Builder setCloseNotifier(Runnable closeNotifier) {
            this.closeNotifier = closeNotifier;
            return this;
        }

        public Builder setPeekNextBufferSubpartitionIdFunction(
                BiFunction<TieredStoragePartitionId, ResultSubpartitionIndexSet, Integer>
                        peekNextBufferSubpartitionIdFunction) {
            this.peekNextBufferSubpartitionIdFunction = peekNextBufferSubpartitionIdFunction;
            return this;
        }

        public TestingTierConsumerAgent build() {
            return new TestingTierConsumerAgent(
                    startNotifier,
                    memoryManagerConsumer,
                    bufferSupplier,
                    availabilityNotifierRegistrationRunnable,
                    updateTierShuffleDescriptorRunnable,
                    closeNotifier,
                    peekNextBufferSubpartitionIdFunction);
        }
    }
}
