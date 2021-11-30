/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.app;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.Ignite;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.lang.LoggerMessageHelper;
import org.apache.ignite.utils.IgniteProperties;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of an entry point for handling grid lifecycle.
 */
public class Ignition {
    /** The logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(Ignition.class);

    private static final String[] BANNER = {
            "",
            "           #              ___                         __",
            "         ###             /   |   ____   ____ _ _____ / /_   ___",
            "     #  #####           / /| |  / __ \\ / __ `// ___// __ \\ / _ \\",
            "   ###  ######         / ___ | / /_/ // /_/ // /__ / / / // ___/",
            "  #####  #######      /_/  |_|/ .___/ \\__,_/ \\___//_/ /_/ \\___/",
            "  #######  ######            /_/",
            "    ########  ####        ____               _  __           _____",
            "   #  ########  ##       /  _/____ _ ____   (_)/ /_ ___     |__  /",
            "  ####  #######  #       / / / __ `// __ \\ / // __// _ \\     /_ <",
            "   #####  #####        _/ / / /_/ // / / // // /_ / ___/   ___/ /",
            "     ####  ##         /___/ \\__, //_/ /_//_/ \\__/ \\___/   /____/",
            "       ##                  /____/\n"
    };

    private static final String VER_KEY = "version";

    /**
     * Node name to node instance mapping. Please pay attention, that nodes in given map might be in any state: STARTING, STARTED, STOPPED.
     */
    private static Map<String, IgniteImpl> nodes = new ConcurrentHashMap<>();

    /**
     * Starts an Ignite node with an optional bootstrap configuration from a HOCON file.
     *
     * @param nodeName Name of the node. Must not be {@code null}.
     * @param cfgPath  Path to the node configuration in the HOCON format. Must not be {@code null}.
     * @param workDir  Work directory for the started node. Must not be {@code null}.
     * @return Started Ignite node.
     */
    public static Ignite start(@NotNull String nodeName, @NotNull Path cfgPath, @NotNull Path workDir) {
        try {
            return doStart(
                    nodeName,
                    Files.readString(cfgPath),
                    workDir
            );
        } catch (IOException e) {
            throw new IgniteException("Unable to read user specific configuration.", e);
        }
    }

    /**
     * Starts an Ignite node with an optional bootstrap configuration from an input stream with HOCON configs.
     *
     * @param nodeName Name of the node. Must not be {@code null}.
     * @param cfg      Optional node configuration based on {@link org.apache.ignite.configuration.schemas.runner.NodeConfigurationSchema}
     *                 and {@link org.apache.ignite.configuration.schemas.network.NetworkConfigurationSchema}. Following rules are used for
     *                 applying the configuration properties:
     *                 <ol>
     *                   <li>Specified property overrides existing one or just applies itself if it wasn't
     *                       previously specified.</li>
     *                   <li>All non-specified properties either use previous value or use default one from
     *                       corresponding configuration schema.</li>
     *                 </ol>
     *                 So that, in case of initial node start (first start ever) specified configuration, supplemented
     *                 with defaults, is used. If no configuration was provided defaults are used for all
     *                 configuration properties. In case of node restart, specified properties override existing
     *                 ones, non specified properties that also weren't specified previously use default values.
     *                 Please pay attention that previously specified properties are searched in the
     *                 {@code workDir} specified by the user.
     * @param workDir  Work directory for the started node. Must not be {@code null}.
     * @return Started Ignite node.
     */
    public static Ignite start(@NotNull String nodeName, @NotNull InputStream cfg, @NotNull Path workDir) {
        try {
            return doStart(
                    nodeName,
                    new String(cfg.readAllBytes(), StandardCharsets.UTF_8),
                    workDir
            );
        } catch (IOException e) {
            throw new IgniteException("Unable to read user specific configuration.", e);
        }
    }

    /**
     * Starts an Ignite node with an optional bootstrap configuration from a HOCON file.
     *
     * @param nodeName  Name of the node. Must not be {@code null}.
     * @param configStr The node configuration in the HOCON format. Must not be {@code null}.
     * @param workDir   Work directory for the started node. Must not be {@code null}.
     * @return Started Ignite node.
     */
    public static Ignite start(@NotNull String nodeName, @NotNull String configStr, @NotNull Path workDir) {
        return doStart(
                nodeName,
                configStr,
                workDir
        );
    }

    /**
     * Starts an Ignite node with the default configuration.
     *
     * @param nodeName Name of the node. Must not be {@code null}.
     * @param workDir  Work directory for the started node. Must not be {@code null}.
     * @return Started Ignite node.
     */
    public static Ignite start(@NotNull String nodeName, @NotNull Path workDir) {
        return doStart(nodeName, null, workDir);
    }

    /**
     * Stops the node with given {@code name}. It's possible to stop both already started node or node that is currently starting. Has no
     * effect if node with specified name doesn't exist.
     *
     * @param name Node name to stop.
     * @throws IllegalArgumentException if null is specified instead of node name.
     */
    public static void stop(@NotNull String name) {
        nodes.computeIfPresent(name, (nodeName, node) -> {
            node.stop();

            return null;
        });
    }

    /**
     * Starts an Ignite node with an optional bootstrap configuration from a HOCON file.
     *
     * @param nodeName   Name of the node. Must not be {@code null}.
     * @param cfgContent Node configuration in the HOCON format. Can be {@code null}.
     * @param workDir    Work directory for the started node. Must not be {@code null}.
     * @return Started Ignite node.
     */
    private static Ignite doStart(String nodeName, @Nullable String cfgContent, Path workDir) {
        if (nodeName.isEmpty()) {
            throw new IllegalArgumentException("Node name must not be null or empty.");
        }

        IgniteImpl nodeToStart = new IgniteImpl(nodeName, workDir);

        IgniteImpl prevNode = nodes.putIfAbsent(nodeName, nodeToStart);

        if (prevNode != null) {
            String errMsg = "Node with name=[" + nodeName + "] already exists.";

            LOG.error(errMsg);

            throw new IgniteException(errMsg);
        }

        ackBanner();

        try {
            nodeToStart.start(cfgContent);
        } catch (Exception e) {
            nodes.remove(nodeName);

            if (e instanceof IgniteException) {
                throw e;
            } else {
                throw new IgniteException(e);
            }
        }

        ackSuccessStart();

        return nodeToStart;
    }

    private static void ackSuccessStart() {
        LOG.info("Apache Ignite started successfully!");
    }

    private static void ackBanner() {
        String ver = IgniteProperties.get(VER_KEY);

        String banner = String.join("\n", BANNER);

        LOG.info(() -> LoggerMessageHelper.format("{}\n" + " ".repeat(22) + "Apache Ignite ver. {}\n", banner, ver), null);
    }
}
