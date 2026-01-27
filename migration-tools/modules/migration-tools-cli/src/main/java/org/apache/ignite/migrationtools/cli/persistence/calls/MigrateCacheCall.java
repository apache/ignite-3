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

package org.apache.ignite.migrationtools.cli.persistence.calls;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.migrationtools.cli.persistence.commands.PersistenceBaseCmd;
import org.apache.ignite.migrationtools.cli.persistence.params.IgniteClientAuthenticatorParams;
import org.apache.ignite.migrationtools.cli.persistence.params.MigrateCacheParams;
import org.apache.ignite.migrationtools.cli.persistence.params.MigrationMode;
import org.apache.ignite.migrationtools.cli.persistence.params.PersistenceParams;
import org.apache.ignite.migrationtools.persistence.Ignite2PersistentCacheTools;
import org.apache.ignite.migrationtools.persistence.MigrationKernalContext;
import org.apache.ignite.migrationtools.persistence.mappers.AbstractSchemaColumnsProcessor;
import org.apache.ignite.migrationtools.persistence.mappers.IgnoreMismatchesSchemaColumnProcessor;
import org.apache.ignite.migrationtools.persistence.mappers.SchemaColumnProcessorStats;
import org.apache.ignite.migrationtools.persistence.mappers.SimpleSchemaColumnsProcessor;
import org.apache.ignite.migrationtools.persistence.mappers.SkipRecordsSchemaColumnsProcessor;
import org.apache.ignite.migrationtools.persistence.utils.pubsub.RateLimiterProcessor;
import org.apache.ignite.migrationtools.sql.SqlDdlGenerator;
import org.apache.ignite.migrationtools.tablemanagement.PersistentTableTypeRegistryImpl;
import org.apache.ignite.migrationtools.tablemanagement.RegisterOnlyTableTypeRegistry;
import org.apache.ignite3.client.BasicAuthenticator;
import org.apache.ignite3.client.IgniteClient;
import org.apache.ignite3.client.IgniteClientConnectionException;
import org.apache.ignite3.internal.cli.core.call.Call;
import org.apache.ignite3.internal.cli.core.call.CallInput;
import org.apache.ignite3.internal.cli.core.call.CallOutput;
import org.apache.ignite3.internal.cli.core.call.CallOutputStatus;
import org.apache.ignite3.internal.cli.core.call.DefaultCallOutput;
import org.apache.ignite3.internal.cli.core.exception.IgniteCliException;
import org.apache.ignite3.internal.cli.logger.CliLoggers;
import org.apache.ignite3.internal.logger.IgniteLogger;
import org.apache.ignite3.internal.util.ExceptionUtils;
import org.apache.ignite3.table.DataStreamerItem;
import org.apache.ignite3.table.Tuple;
import org.jetbrains.annotations.Nullable;

/** Call to the Migrate Cache command. */
public class MigrateCacheCall implements Call<MigrateCacheCall.Input, MigrateCacheCall.Ouput> {
    private static final IgniteLogger LOGGER = CliLoggers.forClass(MigrateCacheCall.class);

    private static Path writeProgressToFile(
            String nodeConsistentId,
            String cacheName,
            List<Map.Entry<Integer, AbstractSchemaColumnsProcessor>> perPartitionColumnProcessors,
            ProgressData resumeFrom) {
        // Update the resumeFrom data;
        perPartitionColumnProcessors.stream()
                .filter(e -> !e.getValue().hasReceivedError())
                .map(e -> new PartitionProgressEntry(e.getKey(), e.getValue().getStats()))
                .forEach(resumeFrom.getCompletedPartitions()::add);

        try {
            Path progressFileToWrite = Files.createTempFile(Path.of("."),
                    "migrate-cache_" + nodeConsistentId + "_" + cacheName + "_", "_progress.json");
            try (var os = Files.newOutputStream(progressFileToWrite)) {
                ObjectMapper objMapper = new ObjectMapper();
                objMapper.writeValue(os, resumeFrom);
            }
            LOGGER.info("ProgressFile saved: {}", progressFileToWrite);

            return progressFileToWrite;
        } catch (IOException ex) {
            LOGGER.error("Could not save progress file: {}", ex);
            return null;
        }
    }

    private static ProgressData loadOrInitProgress(Input i) throws InvalidProgressFileException, IOException {
        String consistentNodeId = i.persistenceParams().nodeConsistentId();
        String cacheName = i.migrateCacheParams().cacheName();

        if (i.migrateCacheParams().progressFileToRead() != null) {
            ProgressData resumeFrom;
            try (var is = Files.newInputStream(i.migrateCacheParams().progressFileToRead())) {
                ObjectMapper objectMapper = new ObjectMapper();
                resumeFrom = objectMapper.readValue(is, ProgressData.class);
            }

            // Check resumeFrom;

            if (!consistentNodeId.equals(resumeFrom.nodeId)) {
                throw InvalidProgressFileException.forNodeConsistentId(resumeFrom.nodeId, i.persistenceParams().nodeConsistentId());
            }

            if (!cacheName.equals(resumeFrom.cacheName)) {
                throw InvalidProgressFileException.forNodeConsistentId(resumeFrom.cacheName, cacheName);
            }

            var migrationModeStr = i.migrateCacheParams().migrationMode().toString();
            if (!migrationModeStr.equals(resumeFrom.migrationMode)) {
                throw InvalidProgressFileException.forMode(resumeFrom.migrationMode, migrationModeStr);
            }

            return resumeFrom;
        } else {
            return new ProgressData(consistentNodeId, cacheName, i.migrateCacheParams().migrationMode().toString(), new ArrayList<>());
        }
    }

    private static <T extends AbstractSchemaColumnsProcessor> SchemaColumnProcessorStats collectStats(
            MigrationMode migrationMode,
            List<Map.Entry<Integer, T>> schemaColumnsProcessors) {
        long processedElements = 0;
        switch (migrationMode) {
            case SKIP_RECORD:
                long numSkippedRecords = 0;
                for (var e : schemaColumnsProcessors) {
                    AbstractSchemaColumnsProcessor p = e.getValue();
                    SkipRecordsSchemaColumnsProcessor.SkippedRecordsStats stats =
                            (SkipRecordsSchemaColumnsProcessor.SkippedRecordsStats) p.getStats();
                    processedElements += stats.getProcessedElements();
                    numSkippedRecords += stats.getNumSkippedRecords();
                }

                return new SkipRecordsSchemaColumnsProcessor.SkippedRecordsStats(processedElements, numSkippedRecords);
            case IGNORE_COLUMN:
                Map<String, Long> droppedCols = new HashMap<>();
                for (var e : schemaColumnsProcessors) {
                    AbstractSchemaColumnsProcessor p = e.getValue();
                    IgnoreMismatchesSchemaColumnProcessor.IgnoredColumnsStats stats =
                            (IgnoreMismatchesSchemaColumnProcessor.IgnoredColumnsStats) p.getStats();
                    processedElements += stats.getProcessedElements();
                    droppedCols.putAll(stats.getDroppedColumns());
                }

                return new IgnoreMismatchesSchemaColumnProcessor.IgnoredColumnsStats(processedElements, droppedCols);
            case PACK_EXTRA:
            case ABORT:
            default:
                for (var e : schemaColumnsProcessors) {
                    AbstractSchemaColumnsProcessor p = e.getValue();
                    SchemaColumnProcessorStats stats = p.getStats();
                    processedElements += stats.getProcessedElements();
                }

                return new SchemaColumnProcessorStats(processedElements);
        }
    }

    @Override
    public CallOutput<Ouput> execute(Input i) {
        // TODO: IGNITE-27619 This must be improved. Sometime picocli gets confused with the args.
        String cacheName = i.migrateCacheParams().cacheName();
        String[] addresses = i.migrateCacheParams().addresses();
        if (addresses[0].equals(cacheName)) {
            addresses = Arrays.copyOfRange(addresses, 1, addresses.length);
        }

        @Nullable BasicAuthenticator authenticator = i.clientAuthenticatorParams().authenticator();

        ProgressData resumeFrom;
        try {
            resumeFrom = loadOrInitProgress(i);
        } catch (InvalidProgressFileException e) {
            LOGGER.error(e.getMessage());
            return DefaultCallOutput.failure(e);
        } catch (IOException e) {
            LOGGER.error("Could not read from progress file: {}", i.migrateCacheParams().progressFileToRead(), e);
            return DefaultCallOutput.failure(
                    new IgniteCliException("Cannot read from progress file: " + i.migrateCacheParams().progressFileToRead(), e));
        }

        List<MigrationKernalContext> persistentCtx = Collections.emptyList();
        List<Map.Entry<Integer, AbstractSchemaColumnsProcessor>> perPartitionColumnProcessors = new ArrayList<>();

        try (var client = IgniteClient.builder()
                .addresses(addresses)
                .authenticator(authenticator)
                .build()) {
            @Nullable IgniteConfiguration cfg = PersistenceBaseCmd.createValidIgniteCfg(i.persistenceParams());
            if (cfg == null) {
                return DefaultCallOutput.failure(new IgniteCliException("Unable to read ignite configuration"));
            }

            persistentCtx = PersistenceBaseCmd.createAndStartMigrationContext(i.persistenceParams(), cfg);
            if (persistentCtx.isEmpty()) {
                return DefaultCallOutput.failure(new IgniteCliException(
                        String.format("Could not find node (consistentId:%s) folder in '%s'", i.persistenceParams().nodeConsistentId(),
                                i.persistenceParams().workDir().toString())
                ));
            }

            MigrationMode migrationMode = i.migrateCacheParams().migrationMode();
            boolean packExtraFields = migrationMode == MigrationMode.PACK_EXTRA;
            Ignite2PersistentCacheTools.ColumnsProcessorFactory processorFactory =
                    (cacheTuplePublisher, partitionId, clientSchema, columnsToFieldsMappings, typeConverters) -> {
                        // Skip partition
                        if (resumeFrom.getCompletedPartitions().stream().anyMatch(e -> e.getPartitionId() == partitionId)) {
                            return null;
                        }

                        Flow.Publisher<DataStreamerItem<Map.Entry<Tuple, Tuple>>> tail;
                        {
                            AbstractSchemaColumnsProcessor processor;
                            switch (migrationMode) {
                                case SKIP_RECORD:
                                    processor =
                                            new SkipRecordsSchemaColumnsProcessor(clientSchema, columnsToFieldsMappings, typeConverters);
                                    break;
                                case IGNORE_COLUMN:
                                    processor = new IgnoreMismatchesSchemaColumnProcessor(clientSchema, columnsToFieldsMappings,
                                            typeConverters);
                                    break;
                                case PACK_EXTRA:
                                case ABORT:
                                default:
                                    processor = new SimpleSchemaColumnsProcessor(clientSchema, columnsToFieldsMappings, typeConverters,
                                            packExtraFields);
                            }
                            perPartitionColumnProcessors.add(Map.entry(partitionId, processor));
                            cacheTuplePublisher.subscribe(processor);
                            tail = processor;
                        }

                        if (i.migrateCacheParams().rateLimiter() > 0) {
                            Flow.Processor<DataStreamerItem<Map.Entry<Tuple, Tuple>>, DataStreamerItem<Map.Entry<Tuple, Tuple>>> rl =
                                    new RateLimiterProcessor<>(1, TimeUnit.SECONDS, i.migrateCacheParams().rateLimiter());
                            tail.subscribe(rl);
                            tail = rl;
                        }

                        return tail;
                    };

            var registry = new RegisterOnlyTableTypeRegistry(new PersistentTableTypeRegistryImpl(client));
            var sqlGenerator = new SqlDdlGenerator(registry, packExtraFields);

            // TODO: IGNITE-27629 Add validation. The cache must exist etc.
            LOGGER.info("Starting the migration process");
            Ignite2PersistentCacheTools.migrateCache(client, sqlGenerator, persistentCtx, cacheName, processorFactory);
            LOGGER.info("Waiting for results to be persisted");

            @Nullable Path outputProgressFile;
            if (!i.migrateCacheParams().saveProgressFileDisabled()) {
                outputProgressFile = writeProgressToFile(
                        i.persistenceParams().nodeConsistentId(),
                        i.migrateCacheParams().cacheName(),
                        perPartitionColumnProcessors,
                        resumeFrom);
            } else {
                outputProgressFile = null;
            }

            SchemaColumnProcessorStats stats = collectStats(migrationMode, perPartitionColumnProcessors);
            LOGGER.info("Finished persisting records: {}", stats);

            return DefaultCallOutput.success(new Ouput("Migration finished successfully", outputProgressFile));
        } catch (Exception e) {
            // Check if there's a better way to check for connection refused and other on connect errors.
            if (e instanceof IgniteClientConnectionException) {
                LOGGER.error("Could not connect to the cluster", e);
                return DefaultCallOutput.failure(e);
            } else {
                // Lets try to write progress.
                @Nullable Path outputProgressFile;
                // TODO: This logic is duplicated.
                if (!i.migrateCacheParams().saveProgressFileDisabled()) {
                    // Let's just remove the last processor, since there is guarantee that it completed successfully.
                    if (!perPartitionColumnProcessors.isEmpty()) {
                        perPartitionColumnProcessors.remove(perPartitionColumnProcessors.size() - 1);
                    }

                    outputProgressFile = writeProgressToFile(
                            i.persistenceParams().nodeConsistentId(),
                            i.migrateCacheParams().cacheName(),
                            perPartitionColumnProcessors,
                            resumeFrom);
                } else {
                    outputProgressFile = null;
                }

                LOGGER.error("Error while migration persistence folder", e);
                return DefaultCallOutput.<Ouput>builder()
                        .status(CallOutputStatus.ERROR)
                        .cause(ExceptionUtils.unwrapCause(e))
                        .body(new Ouput("Migration finished unsuccessfully", outputProgressFile))
                        .build();
            }
        } finally {
            for (var ctx : persistentCtx) {
                try {
                    ctx.stop();
                } catch (IgniteCheckedException e) {
                    LOGGER.warn("Error while closing persistent contexts", e);
                }
            }
        }
    }

    static class PartitionProgressEntry {
        private int partitionId;

        private SchemaColumnProcessorStats stats;

        private PartitionProgressEntry() {
            super();
        }

        private PartitionProgressEntry(int partitionId, SchemaColumnProcessorStats stats) {
            this.partitionId = partitionId;
            this.stats = stats;
        }

        public int getPartitionId() {
            return partitionId;
        }

        public SchemaColumnProcessorStats getStats() {
            return stats;
        }
    }

    private static class ProgressData {
        private String nodeId;

        private String cacheName;

        private String migrationMode;

        private List<PartitionProgressEntry> completedPartitions;

        private ProgressData() {
            super();
        }

        public ProgressData(String nodeId, String cacheName, String migrationMode, List<PartitionProgressEntry> completedPartitions) {
            this.nodeId = nodeId;
            this.cacheName = cacheName;
            this.migrationMode = migrationMode;
            this.completedPartitions = completedPartitions;
        }

        public String getNodeId() {
            return nodeId;
        }

        public String getCacheName() {
            return cacheName;
        }

        public String getMigrationMode() {
            return migrationMode;
        }

        public List<PartitionProgressEntry> getCompletedPartitions() {
            return completedPartitions;
        }
    }

    /** InvalidProgressFileException. */
    public static class InvalidProgressFileException extends Exception {
        public InvalidProgressFileException(String message) {
            super("Progress file does not match the provided arguments: " + message);
        }

        static InvalidProgressFileException forNodeConsistentId(String expected, String actual) {
            return new InvalidProgressFileException("Progress File is from node '" + expected + "', but '" + actual + "' was requested.");
        }

        static InvalidProgressFileException forCacheName(String expected, String actual) {
            return new InvalidProgressFileException("Progress File is from cache '" + expected + "', but '" + actual + "' was requested.");
        }

        static InvalidProgressFileException forMode(String expected, String actual) {
            return new InvalidProgressFileException(
                    "Progress File has migration mode '" + expected + "', but '" + actual + "' was requested.");
        }
    }

    /** Input. */
    public static class Input implements CallInput {
        private final PersistenceParams persistenceParams;

        private final MigrateCacheParams migrateCacheParams;

        private final IgniteClientAuthenticatorParams clientAuthenticatorParams;

        /**
         * Constructor.
         *
         * @param persistenceParams Persistence params.
         * @param migrateCacheParams Migrate Cache params.
         * @param clientAuthenticatorParams Ignite Client authenticator params.
         */
        public Input(
                PersistenceParams persistenceParams,
                MigrateCacheParams migrateCacheParams,
                IgniteClientAuthenticatorParams clientAuthenticatorParams
        ) {
            this.persistenceParams = persistenceParams;
            this.migrateCacheParams = migrateCacheParams;
            this.clientAuthenticatorParams = clientAuthenticatorParams;
        }

        PersistenceParams persistenceParams() {
            return persistenceParams;
        }

        MigrateCacheParams migrateCacheParams() {
            return migrateCacheParams;
        }

        IgniteClientAuthenticatorParams clientAuthenticatorParams() {
            return clientAuthenticatorParams;
        }
    }

    /** Output. */
    public static class Ouput {
        private String msg;

        private Path progressFilePath;

        public Ouput(String msg, Path progressFilePath) {
            this.msg = msg;
            this.progressFilePath = progressFilePath;
        }

        public String getMsg() {
            return msg;
        }

        public Path getProgressFilePath() {
            return progressFilePath;
        }
    }
}
