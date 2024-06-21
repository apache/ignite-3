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

package org.apache.ignite.internal.app;

import static java.lang.System.lineSeparator;

import com.google.auto.service.AutoService;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.InitParameters;
import org.apache.ignite.internal.eventlog.api.IgniteEventType;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.properties.IgniteProductVersion;
import org.apache.ignite.lang.ErrorGroups;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.apache.ignite.lang.IgniteException;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Implementation of an entry point for handling grid lifecycle.
 */
@AutoService(Ignition.class)
public class IgnitionImpl implements Ignition {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(IgnitionImpl.class);

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

    /**
     * Node name to node instance mapping. Please pay attention, that nodes in given map might be in any state: STARTING, STARTED, STOPPED.
     */
    private static final Map<String, IgniteImpl> nodes = new ConcurrentHashMap<>();

    /**
     * Collection of nodes that has been started and are eligible for receiving the init command.
     */
    private static final Map<String, IgniteImpl> readyForInitNodes = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Ignite> start(String nodeName, Path cfgPath, Path workDir) {
        return start(nodeName, cfgPath, workDir, defaultServiceClassLoader());
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Ignite> start(
            String nodeName,
            Path cfgPath,
            Path workDir,
            @Nullable ClassLoader serviceLoaderClassLoader
    ) {
        ErrorGroups.initialize();
        IgniteEventType.initialize();

        Objects.requireNonNull(cfgPath, "Config path must not be null");
        if (Files.notExists(cfgPath)) {
            throw new IgniteException("Config file doesn't exist");
        }

        return doStart(
                nodeName,
                cfgPath,
                workDir,
                serviceLoaderClassLoader
        );
    }

    @Override
    public void stop(String nodeName) {
        readyForInitNodes.remove(nodeName);

        nodes.computeIfPresent(nodeName, (name, node) -> {
            try {
                node.stop();
            } catch (Exception e) {
                throw new IgniteException(Common.NODE_STOPPING_ERR, e);
            }

            return null;
        });
    }

    /**
     * Stops all Ignite instances started in this JVM.
     */
    @TestOnly
    public void stopAll() {
        List<String> nodeNames = new ArrayList<>(nodes.keySet());

        if (!nodeNames.isEmpty()) {
            LOG.info("Going to stop Ignite instances: " + nodeNames);

            for (String nodeName : nodeNames) {
                stop(nodeName);
            }

            LOG.info("Stopped the following Ignite instances: " + nodeNames);
        }
    }

    @Override
    public void init(InitParameters parameters) {
        String nodeName = parameters.nodeName();
        IgniteImpl node = readyForInitNodes.get(nodeName);

        if (node == null) {
            throw new IgniteException("Node \"" + nodeName + "\" has not been started");
        }

        try {
            node.init(parameters.metaStorageNodeNames(),
                    parameters.cmgNodeNames(),
                    parameters.clusterName(),
                    parameters.clusterConfiguration()
            );
        } catch (NodeStoppingException e) {
            throw new IgniteException("Node stop detected during init", e);
        }
    }

    private static ClassLoader defaultServiceClassLoader() {
        return Thread.currentThread().getContextClassLoader();
    }

    /**
     * Starts an Ignite node with an optional bootstrap configuration from a HOCON file.
     *
     * @param nodeName Name of the node. Must not be {@code null}.
     * @param configPath Path to node configuration in the HOCON format. Must not be {@code null}.
     * @param workDir Work directory for the started node. Must not be {@code null}.
     * @return Completable future that resolves into an Ignite node after all components are started and the cluster initialization is
     *         complete.
     */
    private static CompletableFuture<Ignite> doStart(
            String nodeName,
            Path configPath,
            Path workDir,
            @Nullable ClassLoader serviceLoaderClassLoader
    ) {
        if (nodeName.isEmpty()) {
            throw new IllegalArgumentException("Node name must not be null or empty.");
        }

        IgniteImpl nodeToStart = new IgniteImpl(nodeName, configPath, workDir, serviceLoaderClassLoader);

        IgniteImpl prevNode = nodes.putIfAbsent(nodeName, nodeToStart);

        if (prevNode != null) {
            String errMsg = "Node already exists [name=" + nodeName + "]";

            LOG.debug(errMsg);

            throw new IgniteException(errMsg);
        }

        ackBanner();

        try {
            CompletableFuture<Ignite> future = nodeToStart.start(configPath)
                    .handle((ignite, e) -> {
                        if (e == null) {
                            ackSuccessStart();

                            return ignite;
                        } else {
                            throw handleStartException(nodeName, e);
                        }
                    });

            readyForInitNodes.put(nodeName, nodeToStart);

            return future;
        } catch (Exception e) {
            throw handleStartException(nodeName, e);
        }
    }

    private static IgniteException handleStartException(String nodeName, Throwable e) {
        readyForInitNodes.remove(nodeName);
        nodes.remove(nodeName);

        if (e instanceof IgniteException) {
            return (IgniteException) e;
        } else {
            return new IgniteException(e);
        }
    }

    private static void ackSuccessStart() {
        LOG.info("Apache Ignite started successfully!");
    }

    private static void ackBanner() {
        String banner = String.join(lineSeparator(), BANNER);

        String padding = " ".repeat(22);

        String version;

        try (InputStream versionStream = IgnitionImpl.class.getClassLoader().getResourceAsStream("ignite.version.full")) {
            if (versionStream != null) {
                version = new String(versionStream.readAllBytes(), StandardCharsets.UTF_8);
            } else {
                version = IgniteProductVersion.CURRENT_VERSION.toString();
            }
        } catch (IOException e) {
            version = IgniteProductVersion.CURRENT_VERSION.toString();
        }

        LOG.info("{}" + lineSeparator() + "{}{}" + lineSeparator(), banner, padding, "Apache Ignite ver. " + version);
    }
}
