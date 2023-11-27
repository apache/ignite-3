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

import com.typesafe.config.ConfigException;
import com.typesafe.config.parser.ConfigDocument;
import com.typesafe.config.parser.ConfigDocumentFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.InitParameters;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.lang.IgniteException;
import org.jetbrains.annotations.Nullable;

/** Helper class for starting a node with a string-based configuration. */
public class TestIgnitionManager {

    /** Default name of configuration file. */
    public static final String DEFAULT_CONFIG_NAME = "ignite-config.conf";

    private static final int DEFAULT_SCALECUBE_METADATA_TIMEOUT = 10_000;

    /** Default DelayDuration in ms used for tests that is set on node init. */
    public static final int DEFAULT_DELAY_DURATION_MS = 100;

    private static final int DEFAULT_METASTORAGE_IDLE_SYNC_TIME_INTERVAL_MS = 10;

    /** Default partition idle SafeTime interval in ms used for tests that is set on node init. */
    public static final int DEFAULT_PARTITION_IDLE_SYNC_TIME_INTERVAL_MS = 100;

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
    public static CompletableFuture<Ignite> start(String nodeName, @Nullable String configStr, Path workDir) {
        String enrichedConfig = enrichValidConfigWithTestDefaults(configStr);

        try {
            Files.createDirectories(workDir);
            Path configPath = workDir.resolve(DEFAULT_CONFIG_NAME);
            if (configStr == null) {
                // Null config might mean that this is a restart, so we should not rewrite the existing config file.
                if (Files.notExists(configPath)) {
                    Files.createFile(configPath);
                }
            } else {
                assert enrichedConfig != null;

                Files.writeString(configPath, enrichedConfig,
                        StandardOpenOption.SYNC, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            }

            return IgnitionManager.start(nodeName, configPath, workDir);
        } catch (IOException e) {
            throw new IgniteException("Couldn't write node config.", e);
        }
    }

    private static @Nullable String enrichValidConfigWithTestDefaults(@Nullable String configStr) {
        try {
            return enrichConfigWithTestDefaults(configStr);
        } catch (ConfigException e) {
            // Config is invalid, let Ignite itself reject it in a predictable way.
            return configStr;
        }
    }

    private static String enrichConfigWithTestDefaults(@Nullable String configStr) {
        ConfigDocument configDocument = parseNullableConfigString(configStr);

        configDocument = applyTestDefault(
                configDocument,
                "network.membership.scaleCube.metadataTimeout",
                Integer.toString(DEFAULT_SCALECUBE_METADATA_TIMEOUT)
        );

        return configDocument.render();
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

        ConfigDocument configDocument = parseNullableConfigString(params.clusterConfiguration());

        configDocument = applyTestDefault(
                configDocument,
                "schemaSync.delayDuration",
                Integer.toString(DEFAULT_DELAY_DURATION_MS)
        );
        configDocument = applyTestDefault(
                configDocument,
                "metaStorage.idleSyncTimeInterval",
                Integer.toString(DEFAULT_METASTORAGE_IDLE_SYNC_TIME_INTERVAL_MS)
        );
        configDocument = applyTestDefault(
                configDocument,
                "replication.idleSafeTimePropagationDuration",
                Integer.toString(DEFAULT_PARTITION_IDLE_SYNC_TIME_INTERVAL_MS)
        );

        builder.clusterConfiguration(configDocument.render());

        return builder.build();
    }

    private static ConfigDocument parseNullableConfigString(@Nullable String configString) {
        String configToParse = Objects.requireNonNullElse(configString, "{}");

        return ConfigDocumentFactory.parseString(configToParse);
    }

    private static ConfigDocument applyTestDefault(ConfigDocument document, String path, String value) {
        if (document.hasPath(path)) {
            return document;
        } else {
            return document.withValueText(path, value);
        }
    }
}
