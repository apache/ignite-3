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

import static java.lang.System.lineSeparator;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.properties.IgniteProductVersion;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.lang.NodeStoppingException;
import org.intellij.lang.annotations.Language;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of an entry point for handling grid lifecycle.
 */
public class IgnitionImpl implements Ignition {
    /** The logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(IgnitionImpl.class);

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
    public CompletableFuture<Ignite> start(String nodeName, @Nullable Path cfgPath, Path workDir) {
        return start(nodeName, cfgPath, workDir, defaultServiceClassLoader());
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Ignite> start(
            String nodeName,
            @Nullable Path cfgPath,
            Path workDir,
            @Nullable ClassLoader serviceLoaderClassLoader
    ) {
        try {
            return doStart(
                    nodeName,
                    cfgPath == null ? null : Files.readString(cfgPath),
                    workDir,
                    serviceLoaderClassLoader
            );
        } catch (IOException e) {
            throw new IgniteException("Unable to read user specific configuration.", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Ignite> start(String nodeName, @Nullable URL cfgUrl, Path workDir) {
        if (cfgUrl == null) {
            return start(nodeName, workDir);
        } else {
            try (InputStream cfgStream = cfgUrl.openStream()) {
                return start(nodeName, cfgStream, workDir);
            } catch (IOException e) {
                throw new IgniteException("Unable to read user specific configuration.", e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Ignite> start(String nodeName, @Nullable InputStream cfg, Path workDir) {
        try {
            return doStart(
                    nodeName,
                    cfg == null ? null : new String(cfg.readAllBytes(), StandardCharsets.UTF_8),
                    workDir,
                    defaultServiceClassLoader()
            );
        } catch (IOException e) {
            throw new IgniteException("Unable to read user specific configuration.", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Ignite> start(String nodeName, Path workDir) {
        return doStart(nodeName, null, workDir, defaultServiceClassLoader());
    }

    /** {@inheritDoc} */
    @Override
    public void stop(String nodeName) {
        readyForInitNodes.remove(nodeName);

        nodes.computeIfPresent(nodeName, (name, node) -> {
            node.stop();

            return null;
        });
    }

    @Override
    public void init(String nodeName, Collection<String> metaStorageNodenodeNames, String clusterName) {
        init(nodeName, metaStorageNodenodeNames, List.of(), clusterName);
    }

    @Override
    public void init(
            String nodeName,
            Collection<String> metaStorageNodeNames,
            Collection<String> cmgNodeNames,
            String clusterName
    ) {
        IgniteImpl node = readyForInitNodes.get(nodeName);

        if (node == null) {
            throw new IgniteException("Node \"" + nodeName + "\" has not been started");
        }

        try {
            node.init(metaStorageNodeNames, cmgNodeNames, clusterName);
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
     * @param cfgContent Node configuration in the HOCON format. Can be {@code null}.
     * @param workDir Work directory for the started node. Must not be {@code null}.
     * @return Completable future that resolves into an Ignite node after all components are started and the cluster initialization is
     *         complete.
     */
    private static CompletableFuture<Ignite> doStart(
            String nodeName,
            @Language("HOCON") @Nullable String cfgContent,
            Path workDir,
            @Nullable ClassLoader serviceLoaderClassLoader
    ) {
        if (nodeName.isEmpty()) {
            throw new IllegalArgumentException("Node name must not be null or empty.");
        }

        IgniteImpl nodeToStart = new IgniteImpl(nodeName, workDir, serviceLoaderClassLoader);

        IgniteImpl prevNode = nodes.putIfAbsent(nodeName, nodeToStart);

        if (prevNode != null) {
            String errMsg = "Node with name=[" + nodeName + "] already exists.";

            LOG.error(errMsg);

            throw new IgniteException(errMsg);
        }

        ackBanner();

        try {
            CompletableFuture<Ignite> future = nodeToStart.start(cfgContent)
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

        String version = "Apache Ignite ver. " + IgniteProductVersion.CURRENT_VERSION;

        LOG.info("{}" + lineSeparator() + "{}{}" + lineSeparator(), banner, padding, version);
    }
}
