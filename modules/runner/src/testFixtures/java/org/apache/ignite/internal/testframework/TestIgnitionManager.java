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

package org.apache.ignite.internal.testframework;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.SYNC;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.util.Map.entry;
import static org.apache.ignite.internal.util.Constants.MiB;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;

import com.typesafe.config.ConfigException;
import com.typesafe.config.parser.ConfigDocument;
import com.typesafe.config.parser.ConfigDocumentFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteServer;
import org.apache.ignite.InitParameters;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.lang.IgniteException;
import org.jetbrains.annotations.Nullable;

/** Helper class for starting a node with a string-based configuration. */
public class TestIgnitionManager {

    /** Default name of configuration file. */
    public static final String DEFAULT_CONFIG_NAME = "ignite-config.conf";

    public static final int DEFAULT_SCALECUBE_METADATA_TIMEOUT = 10_000;

    /** Default DelayDuration in ms used for tests that is set on node init. */
    public static final int DEFAULT_DELAY_DURATION_MS = 100;

    private static final int DEFAULT_METASTORAGE_IDLE_SAFE_TIME_SYNC_INTERVAL_MS = 50;

    /** Default partition idle SafeTime interval in ms used for tests that is set on node init. */
    public static final int DEFAULT_PARTITION_IDLE_SYNC_TIME_INTERVAL_MS = 100;

    public static final long DEFAULT_MAX_CLOCK_SKEW_MS = TestClockService.TEST_MAX_CLOCK_SKEW_MILLIS;

    /** Map with default node configuration values. */
    private static final Map<String, String> DEFAULT_NODE_CONFIG = Map.ofEntries(
            entry("ignite.network.membership.scaleCube.metadataTimeoutMillis", Integer.toString(DEFAULT_SCALECUBE_METADATA_TIMEOUT)),
            entry("ignite.system.properties.aipersistThrottling", "disabled"),
            entry("ignite.system.criticalWorkers.maxAllowedLagMillis", "500"),
            entry("ignite.system.criticalWorkers.livenessCheckIntervalMillis", "200"),
            entry("ignite.system.criticalWorkers.nettyThreadsHeartbeatIntervalMillis", "100")
    );

    /** Map of pre-configured by default storage profiles. */
    private static final Map<String, String> DEFAULT_STORAGE_PROFILES = Map.ofEntries(
            entry("ignite.storage.profiles.default_aipersist.engine", "aipersist"),
            entry("ignite.storage.profiles.default_aipersist.sizeBytes", Integer.toString(256 * MiB)),
            entry("ignite.storage.profiles.default_aimem.engine", "aimem"),
            entry("ignite.storage.profiles.default_aimem.initSizeBytes", Integer.toString(256 * MiB)),
            entry("ignite.storage.profiles.default_aimem.maxSizeBytes", Integer.toString(256 * MiB)),
            entry("ignite.storage.profiles.default_rocksdb.engine", "rocksdb"),
            entry("ignite.storage.profiles.default_rocksdb.sizeBytes", Integer.toString(256 * MiB)),
            entry("ignite.storage.profiles.default.engine", "aipersist"),
            entry("ignite.storage.profiles.default.sizeBytes", Integer.toString(256 * MiB))
    );

    /** Map with default cluster configuration values. */
    private static final Map<String, String> DEFAULT_CLUSTER_CONFIG = Map.of(
            "ignite.schemaSync.delayDurationMillis", Integer.toString(DEFAULT_DELAY_DURATION_MS),
            "ignite.schemaSync.maxClockSkewMillis", Long.toString(DEFAULT_MAX_CLOCK_SKEW_MS),
            "ignite.system.idleSafeTimeSyncIntervalMillis", Integer.toString(DEFAULT_METASTORAGE_IDLE_SAFE_TIME_SYNC_INTERVAL_MS),
            "ignite.replication.idleSafeTimePropagationDurationMillis", Integer.toString(DEFAULT_PARTITION_IDLE_SYNC_TIME_INTERVAL_MS)
    );

    /**
     * Marker that explicitly requests production defaults when put to {@link InitParametersBuilder#clusterConfiguration(String)}.
     */
    public static final String PRODUCTION_CLUSTER_CONFIG_STRING = "<production-cluster-config>";

    /**
     * Starts an Ignite node with an optional bootstrap configuration from an input stream with HOCON configs.
     *
     * <p>Test defaults are mixed to the configuration (only if the corresponding config keys are not explicitly defined).
     *
     * <p>When this method returns, the node is partially started and ready to accept the init command (that is, its
     * REST endpoint is functional).
     *
     * @param nodeName Name of the node. Must not be {@code null}.
     * @param configStr Optional node configuration.
     *      Following rules are used for applying the configuration properties:
     *      <ol>
     *        <li>Specified property overrides existing one or just applies itself if it wasn't
     *            previously specified.</li>
     *        <li>All non-specified properties either use previous value or use default one from
     *            corresponding configuration schema.</li>
     *      </ol>
     *      So that, in case of initial node start (first start ever) specified configuration, supplemented
     *      with defaults, is used. If no configuration was provided defaults are used for all
     *      configuration properties. In case of node restart, specified properties override existing
     *      ones, non specified properties that also weren't specified previously use default values.
     *      Please pay attention that previously specified properties are searched in the
     *      {@code workDir} specified by the user.
     *
     * @param workDir Work directory for the started node. Must not be {@code null}.
     * @return Completable future that resolves into an Ignite node after all components are started and the cluster initialization is
     *         complete.
     * @throws IgniteException If error occurs while reading node configuration.
     */
    public static IgniteServer start(String nodeName, @Nullable String configStr, Path workDir) {
        return doStart(nodeName, configStr, workDir, true, true);
    }

    /**
     * The same as {@link TestIgnitionManager#start(String, String, Path)}, but the node wouldn't use pre-configured set of storage profiles
     * for each storage engine from {@link TestIgnitionManager#DEFAULT_STORAGE_PROFILES}.
     *
     * @param nodeName Name of the node. Must not be {@code null}.
     * @param configStr Optional node configuration.
     *      Following rules are used for applying the configuration properties:
     *      <ol>
     *        <li>Specified property overrides existing one or just applies itself if it wasn't
     *            previously specified.</li>
     *        <li>All non-specified properties either use previous value or use default one from
     *            corresponding configuration schema.</li>
     *      </ol>
     *      So that, in case of initial node start (first start ever) specified configuration, supplemented
     *      with defaults, is used. If no configuration was provided defaults are used for all
     *      configuration properties. In case of node restart, specified properties override existing
     *      ones, non specified properties that also weren't specified previously use default values.
     *      Please pay attention that previously specified properties are searched in the
     *      {@code workDir} specified by the user.
     *
     * @param workDir Work directory for the started node. Must not be {@code null}.
     * @return Completable future that resolves into an Ignite node after all components are started and the cluster initialization is
     *         complete.
     * @throws IgniteException If error occurs while reading node configuration.
     */
    public static IgniteServer startWithoutPreConfiguredStorageProfiles(String nodeName, @Nullable String configStr, Path workDir) {
        return doStart(nodeName, configStr, workDir, true, false);
    }

    /**
     * Starts an Ignite node with an optional bootstrap configuration defined as a HOCON (Human-Optimized Config Object 
     * Notation) configuration string.
     *
     * <p>When this method returns, the node is partially started, meaning its REST endpoint is functional and it is ready 
     * to accept the initialization command (e.g., for setting up the cluster).
     *
     * @param nodeName The name of the node to start. Must not be {@code null} and must be unique within the cluster.
     * @param configStr An optional configuration string for the node written in HOCON format. If {@code null}, the 
     *                  default configuration will be used.
     * @param workDir The path to the working directory for the node. This directory must exist and the application must 
     *                have sufficient permissions to read from and write to it.
     * @return A {@link CompletableFuture} that resolves to the fully initialized Ignite node with all components started and
     *         the cluster ready for further interactions.
     * @throws IgniteException If an error occurs while reading or parsing the node configuration.
     * @throws IllegalArgumentException If {@code nodeName} is {@code null} or invalid.
     */
    public static IgniteServer startWithProductionDefaults(String nodeName, @Nullable String configStr, Path workDir) {
        return doStart(nodeName, configStr, workDir, false, false);
    }

    private static IgniteServer doStart(
            String nodeName,
            @Nullable String configStr,
            Path workDir,
            boolean useTestDefaults,
            boolean usePreConfiguredStorageProfiles
    ) {
        try {
            Files.createDirectories(workDir);
            Path configPath = workDir.resolve(DEFAULT_CONFIG_NAME);

            if (useTestDefaults) {
                writeConfigurationFileApplyingTestDefaults(configStr, configPath, usePreConfiguredStorageProfiles);
            } else {
                writeConfigurationFile(configStr, configPath);
            }

            return IgniteServer.start(nodeName, configPath, workDir);
        } catch (IOException e) {
            throw new IgniteException(INTERNAL_ERR, "Couldn't write node config.", e);
        }
    }

    /**
     * Writes default values into the configuration file, according to the same rules that are used in {@link #start(String, String, Path)}.
     */
    public static void writeConfigurationFileApplyingTestDefaults(Path configPath) {
        try {
            writeConfigurationFileApplyingTestDefaults(null, configPath, true);
        } catch (IOException e) {
            throw new IgniteException(INTERNAL_ERR, "Couldn't update node configuration file", e);
        }
    }

    private static void writeConfigurationFileApplyingTestDefaults(
            @Nullable String configStr,
            Path configPath,
            boolean useDefaultStorageProfiles
    ) throws IOException {
        Map<String, String> storageProfiles = useDefaultStorageProfiles ? DEFAULT_STORAGE_PROFILES : Map.of();
        writeConfigurationFileApplyingTestDefaults(configStr, configPath, DEFAULT_NODE_CONFIG, storageProfiles);
    }

    /**
     * Applies overrides to the config and writes it to disk.
     *
     * @param configStr Config string.
     * @param configPath Config file path.
     * @param defaults Map of overrides.
     * @param storageProfiles Map of storage profiles overrides.
     * @throws IOException If failed to write the file.
     */
    public static void writeConfigurationFileApplyingTestDefaults(
            @Nullable String configStr,
            Path configPath,
            Map<String, String> defaults,
            Map<String, String> storageProfiles
    ) throws IOException {
        if (configStr == null && Files.exists(configPath)) {
            // Nothing to do.
            return;
        }
        Map<String, String> preconfiguredParams = new HashMap<>(defaults);
        preconfiguredParams.putAll(storageProfiles);

        String configStringToWrite = applyTestDefaultsToConfig(configStr, preconfiguredParams);

        writeConfigurationFile(configStringToWrite, configPath);
    }

    /**
     * Writes config to file.
     *
     * @param configStr Config string.
     * @param configPath Config file path.
     */
    public static void writeConfigurationFile(@Nullable String configStr, Path configPath) throws IOException {
        if (configStr == null && Files.exists(configPath)) {
            // Nothing to do.
            return;
        }

        Files.writeString(configPath, configStr, SYNC, CREATE, TRUNCATE_EXISTING);
    }

    /**
     * Initializes a cluster using test defaults for cluster configuration values that are not
     * specified explicitly.
     *
     * @param parameters Init parameters.
     * @see IgniteServer#initCluster(InitParameters)
     */
    public static void init(IgniteServer node, InitParameters parameters) {
        node.initCluster(applyTestDefaultsToClusterConfig(parameters));
    }

    private static InitParameters applyTestDefaultsToClusterConfig(InitParameters params) {
        InitParametersBuilder builder = new InitParametersBuilder()
                .clusterName(params.clusterName())
                .metaStorageNodeNames(params.metaStorageNodeNames().toArray(String[]::new))
                .cmgNodeNames(params.cmgNodeNames().toArray(String[]::new));

        if (!PRODUCTION_CLUSTER_CONFIG_STRING.equals(params.clusterConfiguration())) {
            builder.clusterConfiguration(applyTestDefaultsToConfig(params.clusterConfiguration(), DEFAULT_CLUSTER_CONFIG));
        }

        return builder.build();
    }

    private static String applyTestDefaultsToConfig(@Nullable String configStr, Map<String, String> defaults) {
        List<Function<ConfigDocument, ConfigDocument>> modifiers = defaults.entrySet().stream()
                .map(TestIgnitionManager::applyTestDefaultModifier)
                .collect(Collectors.toList());

        return applyToConfigEntry(configStr, modifiers);
    }

    /**
     * Applies overrides to the config.
     *
     * @param configStr Config string.
     * @param overrides Map of overrides.
     * @return Rendered config with applied overrides.
     */
    public static String applyOverridesToConfig(@Nullable String configStr, Map<String, String> overrides) {
        List<Function<ConfigDocument, ConfigDocument>> modifiers = overrides.entrySet().stream()
                .map(TestIgnitionManager::applyOverrideModifier)
                .collect(Collectors.toList());

        return applyToConfigEntry(configStr, modifiers);
    }

    private static String applyToConfigEntry(@Nullable String configStr, List<Function<ConfigDocument, ConfigDocument>> modifiers) {
        if (configStr == null) {
            configStr = "{}";
        }

        ConfigDocument configDocument;

        try {
            configDocument = ConfigDocumentFactory.parseString(configStr);
        } catch (ConfigException e) {
            // Preserve original broken content, it might be broken on purpose.
            return configStr;
        }

        for (Function<ConfigDocument, ConfigDocument> override : modifiers) {
            configDocument = override.apply(configDocument);
        }

        return configDocument.render();
    }

    private static Function<ConfigDocument, ConfigDocument> applyTestDefaultModifier(Entry<String, String> entry) {
        return configDocument -> applyTestDefault(configDocument, entry.getKey(), entry.getValue());
    }

    private static ConfigDocument applyTestDefault(ConfigDocument document, String path, String value) {
        if (document.hasPath(path)) {
            return document;
        } else {
            return document.withValueText(path, value);
        }
    }

    private static Function<ConfigDocument, ConfigDocument> applyOverrideModifier(Entry<String, String> entry) {
        return configDocument -> configDocument.withValueText(entry.getKey(), entry.getValue());
    }
}
