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

package org.apache.flink.connector.base.source.reader.fetcher;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;

import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * A Fetcher Manager with a single fetching thread (I/O thread) that handles all splits
 * concurrently.
 *
 * <p>This pattern is, for example, useful for connectors like File Readers, Apache Kafka Readers,
 * etc. In the example of Kafka, there is a single thread that reads all splits (topic partitions)
 * via the same client. In the example of the file source, there is a single thread that reads the
 * files after another.
 */
@PublicEvolving
public class SingleThreadFetcherManager<E, SplitT extends SourceSplit>
        extends SplitFetcherManager<E, SplitT> {

    /**
     * Creates a new SplitFetcherManager with a single I/O threads.
     *
     * @param splitReaderSupplier The factory for the split reader that connects to the source
     *     system.
     */
    public SingleThreadFetcherManager(Supplier<SplitReader<E, SplitT>> splitReaderSupplier) {
        super(splitReaderSupplier, new Configuration());
    }

    /**
     * Creates a new SplitFetcherManager with a single I/O threads.
     *
     * @param splitReaderSupplier The factory for the split reader that connects to the source
     *     system.
     * @param configuration The configuration to create the fetcher manager.
     */
    public SingleThreadFetcherManager(
            Supplier<SplitReader<E, SplitT>> splitReaderSupplier, Configuration configuration) {
        super(splitReaderSupplier, configuration);
    }

    /**
     * Creates a new SplitFetcherManager with a single I/O threads.
     *
     * @param splitReaderSupplier The factory for the split reader that connects to the source
     *     system.
     * @param configuration The configuration to create the fetcher manager.
     * @param splitFinishedHook Hook for handling finished splits in split fetchers
     */
    public SingleThreadFetcherManager(
            Supplier<SplitReader<E, SplitT>> splitReaderSupplier,
            Configuration configuration,
            Consumer<Collection<String>> splitFinishedHook) {
        super(splitReaderSupplier, configuration, splitFinishedHook);
    }

    @Override
    public void addSplits(List<SplitT> splitsToAdd) {
        SplitFetcher<E, SplitT> fetcher = getRunningFetcher();
        if (fetcher == null) {
            fetcher = createSplitFetcher();
            // Add the splits to the fetchers.
            fetcher.addSplits(splitsToAdd);
            startFetcher(fetcher);
        } else {
            fetcher.addSplits(splitsToAdd);
        }
    }

    @Override
    public void removeSplits(List<SplitT> splitsToRemove) {
        SplitFetcher<E, SplitT> fetcher = getRunningFetcher();
        if (fetcher != null) {
            fetcher.removeSplits(splitsToRemove);
        }
    }

    protected SplitFetcher<E, SplitT> getRunningFetcher() {
        return fetchers.isEmpty() ? null : fetchers.values().iterator().next();
    }
}
