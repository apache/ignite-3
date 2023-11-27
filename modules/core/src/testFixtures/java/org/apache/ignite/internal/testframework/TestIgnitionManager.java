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

import com.typesafe.config.parser.ConfigDocument;
import com.typesafe.config.parser.ConfigDocumentFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.InitParameters;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.internal.util.Constants;
import org.apache.ignite.lang.IgniteException;
import org.jetbrains.annotations.Nullable;

/** Helper class for starting a node with a string-based configuration. */
public class TestIgnitionManager {

    /** Default name of configuration file. */
    public static final String DEFAULT_CONFIG_NAME = "ignite-config.conf";

    /** Default DelayDuration in ms used for tests that is set on node init. */
    public static final int DEFAULT_DELAY_DURATION_MS = 100;

    private static final int DEFAULT_METASTORAGE_IDLE_SYNC_TIME_INTERVAL_MS = 10;

    /** Default partition idle SafeTime interval in ms used for tests that is set on node init. */
    public static final int DEFAULT_PARTITION_IDLE_SYNC_TIME_INTERVAL_MS = 100;

    /** Map with default node configuration values. */
    private static final Map<String, String> DEFAULT_NODE_CONFIG = Map.of(
            "aipersist.defaultRegion.size", Integer.toString(256 * Constants.MiB),
            "aimem.defaultRegion.initSize", Integer.toString(256 * Constants.MiB),
            "aimem.defaultRegion.maxSize", Integer.toString(256 * Constants.MiB)
    );

    /** Map with default cluster configuration values. */
    private static final Map<String, String> DEFAULT_CLUSTER_CONFIG = Map.of(
            "schemaSync.delayDuration", Integer.toString(DEFAULT_DELAY_DURATION_MS),
            "metaStorage.idleSyncTimeInterval", Integer.toString(DEFAULT_METASTORAGE_IDLE_SYNC_TIME_INTERVAL_MS),
            "replication.idleSafeTimePropagationDuration", Integer.toString(DEFAULT_PARTITION_IDLE_SYNC_TIME_INTERVAL_MS)
    );

    /**
     * Starts an Ignite node with an optional bootstrap configuration from an input stream with HOCON configs.
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
    public static CompletableFuture<Ignite> start(String nodeName, @Nullable String configStr, Path workDir) {
        try {
            Files.createDirectories(workDir);
            Path configPath = workDir.resolve(DEFAULT_CONFIG_NAME);

            addDefaultsToConfigurationFile(configStr, configPath);

            return IgnitionManager.start(nodeName, configPath, workDir);
        } catch (IOException e) {
            throw new IgniteException("Couldn't write node config.", e);
        }
    }

    /**
     * Writes default values into the configuration file, according to the same rules that are used in {@link #start(String, String, Path)}.
     */
    public static void addDefaultsToConfigurationFile(Path configPath) {
        try {
            addDefaultsToConfigurationFile(null, configPath);
        } catch (IOException e) {
            throw new IgniteException("Couldn't update node configuration file", e);
        }
    }

    private static void addDefaultsToConfigurationFile(@Nullable String configStr, Path configPath) throws IOException {
        Files.writeString(configPath, applyTestDefaultsToConfig(configStr, DEFAULT_NODE_CONFIG),
                StandardOpenOption.SYNC, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    /**
     * Initializes a cluster using test defaults for cluster configuration values that are not
     * specified explicitly.
     *
     * @param parameters Init parameters.
     * @see IgnitionManager#init(InitParameters)
     */
    public static void init(InitParameters parameters) {
        IgnitionManager.init(applyTestDefaultsToClusterConfig(parameters));
    }

    private static InitParameters applyTestDefaultsToClusterConfig(InitParameters params) {
        InitParametersBuilder builder = new InitParametersBuilder()
                .clusterName(params.clusterName())
                .destinationNodeName(params.nodeName())
                .metaStorageNodeNames(params.metaStorageNodeNames())
                .cmgNodeNames(params.cmgNodeNames());

        builder.clusterConfiguration(applyTestDefaultsToConfig(params.clusterConfiguration(), DEFAULT_CLUSTER_CONFIG));

        return builder.build();
    }

    private static String applyTestDefaultsToConfig(@Nullable String configStr, Map<String, String> defaults) {
        ConfigDocument configDocument;

        if (configStr == null) {
            configDocument = ConfigDocumentFactory.parseString("{}");
        } else {
            configDocument = ConfigDocumentFactory.parseString(configStr);
        }

        for (Entry<String, String> entry : defaults.entrySet()) {
            configDocument = applyTestDefault(
                    configDocument,
                    entry.getKey(),
                    entry.getValue()
            );
        }

        return configDocument.render();
    }

    private static ConfigDocument applyTestDefault(ConfigDocument document, String path, String value) {
        if (document.hasPath(path)) {
            return document;
        } else {
            return document.withValueText(path, value);
        }
    }
}
