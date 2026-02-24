/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.metrics.logstorage;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.randomBytes;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.IgniteUtils.startAsync;
import static org.apache.ignite.internal.util.IgniteUtils.stopAsync;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.notNullValue;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metrics.LongGauge;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.metrics.TestMetricManager;
import org.apache.ignite.internal.raft.storage.LogStorageManager;
import org.apache.ignite.internal.raft.storage.impl.VolatileLogStorageManagerCreator;
import org.apache.ignite.internal.raft.util.SharedLogStorageManagerUtils;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.raft.jraft.conf.ConfigurationManager;
import org.apache.ignite.raft.jraft.entity.EnumOutter.EntryType;
import org.apache.ignite.raft.jraft.entity.LogEntry;
import org.apache.ignite.raft.jraft.entity.LogId;
import org.apache.ignite.raft.jraft.entity.codec.DefaultLogEntryCodecFactory;
import org.apache.ignite.raft.jraft.option.LogStorageOptions;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.storage.LogStorage;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(WorkDirectoryExtension.class)
class LogStorageMetricsTest {
    @WorkDirectory
    private Path workDir;

    private LogStorageManager cmgLogStorageManager;
    private LogStorageManager metastorageLogStorageManager;
    private LogStorageManager partitionsLogStorageManager;

    private VolatileLogStorageManagerCreator volatileLogStorageManagerCreator;

    private final TestMetricManager metricManager = new TestMetricManager();

    private LogStorageMetrics logStorageMetrics;

    private final RaftOptions raftOptions = new RaftOptions();

    private final LogStorageOptions logStorageOptions = new LogStorageOptions();

    @BeforeEach
    void setUp() {
        logStorageOptions.setConfigurationManager(new ConfigurationManager());
        logStorageOptions.setLogEntryCodecFactory(DefaultLogEntryCodecFactory.getInstance());

        String nodeName = "test";

        cmgLogStorageManager = SharedLogStorageManagerUtils.create(nodeName, workDir.resolve("cmg"));
        metastorageLogStorageManager = SharedLogStorageManagerUtils.create(nodeName, workDir.resolve("metastorage"));
        partitionsLogStorageManager = SharedLogStorageManagerUtils.create(nodeName, workDir.resolve("partitions"));

        volatileLogStorageManagerCreator = new VolatileLogStorageManagerCreator(nodeName, workDir.resolve("spillout"));

        logStorageMetrics = new LogStorageMetrics(
                nodeName,
                metricManager,
                cmgLogStorageManager,
                metastorageLogStorageManager,
                partitionsLogStorageManager,
                volatileLogStorageManagerCreator,
                10
        );

        CompletableFuture<Void> startFuture = startAsync(
                new ComponentContext(),
                cmgLogStorageManager,
                metastorageLogStorageManager,
                partitionsLogStorageManager,
                volatileLogStorageManagerCreator,
                logStorageMetrics
        );
        assertThat(startFuture, willCompleteSuccessfully());
    }

    @AfterEach
    void cleanup() {
        CompletableFuture<Void> stopFuture = stopAsync(
                new ComponentContext(),
                logStorageMetrics,
                volatileLogStorageManagerCreator,
                partitionsLogStorageManager,
                metastorageLogStorageManager,
                cmgLogStorageManager
        );
        assertThat(stopFuture, willCompleteSuccessfully());
    }

    @Test
    void cmgLogStorageSizeIsAccountedFor() {
        testLogStorageSizeIsAccountedFor(cmgLogStorageManager, "cmg", "CmgLogStorageSize");
    }

    private void testLogStorageSizeIsAccountedFor(LogStorageManager logStorageManager, String groupUri, String metricName) {
        LogStorage logStorage = logStorageManager.createLogStorage(groupUri, raftOptions);
        logStorage.init(logStorageOptions);

        logStorage.appendEntry(dataLogEntry(1, randomBytes(new Random(), 1000)));

        waitForLongGaugeValue(metricName, is(greaterThanOrEqualTo(1000L)));
        waitForLongGaugeValue("TotalLogStorageSize", is(greaterThanOrEqualTo(1000L)));
    }

    private static LogEntry dataLogEntry(int index, byte[] content) {
        LogEntry logEntry = new LogEntry();

        logEntry.setId(new LogId(index, 1));
        logEntry.setType(EntryType.ENTRY_TYPE_DATA);
        logEntry.setData(ByteBuffer.wrap(content));

        return logEntry;
    }

    private void waitForLongGaugeValue(String metricName, Matcher<Long> valueMatcher) {
        Metric metric = metricManager.metric(LogStorageMetricSource.NAME, metricName);
        assertThat(metric, isA(LongGauge.class));
        LongGauge gauge = (LongGauge) metric;

        assertThat(gauge, is(notNullValue()));

        await().until(gauge::value, valueMatcher);
    }

    @Test
    void metastorageLogStorageSizeIsAccountedFor() {
        testLogStorageSizeIsAccountedFor(metastorageLogStorageManager, "metastorage", "MetastorageLogStorageSize");
    }

    @Test
    void partitionsLogStorageSizeIsAccountedFor() {
        testLogStorageSizeIsAccountedFor(partitionsLogStorageManager, new ZonePartitionId(1, 0).toString(), "PartitionsLogStorageSize");
    }
}
