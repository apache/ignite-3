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
            if (configStr == null) {
                if (Files.notExists(configPath)) {
                    Files.createFile(configPath);
                }
            } else {
                Files.writeString(configPath, configStr,
                        StandardOpenOption.SYNC, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            }
            return IgnitionManager.start(nodeName, configPath, workDir);
        } catch (IOException e) {
            throw new IgniteException("Couldn't write node config.", e);
        }
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

        if (params.clusterConfiguration() == null) {
            builder.clusterConfiguration("{ schemaSync.delayDuration: 0 }");
        } else {
            ConfigDocument configDocument = ConfigDocumentFactory.parseString(params.clusterConfiguration());

            String delayDurationPath = "schemaSync.delayDuration";

            if (!configDocument.hasPath(delayDurationPath)) {
                ConfigDocument updatedDocument = configDocument.withValueText(delayDurationPath, "0");

                builder.clusterConfiguration(updatedDocument.render());
            }
        }

        return builder.build();
    }
}
