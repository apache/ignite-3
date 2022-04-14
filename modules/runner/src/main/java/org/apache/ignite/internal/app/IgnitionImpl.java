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
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.utils.IgniteProperties;
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

    private static final String VER_KEY = "version";

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
    public CompletableFuture<Ignite> start(String name, @Nullable URL cfgUrl, Path workDir) {
        if (cfgUrl == null) {
            return doStart(name, null, workDir, defaultServiceClassLoader());
        } else {
            try (InputStream cfgStream = cfgUrl.openStream()) {
                return doStart(name, new String(cfgStream.readAllBytes(), StandardCharsets.UTF_8), workDir, defaultServiceClassLoader());
            } catch (IOException e) {
                throw new IgniteException("Unable to read user specific configuration.", e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Ignite> start(String name, @Nullable InputStream cfg, Path workDir) {
        try {
            return doStart(
                    name,
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
    public CompletableFuture<Ignite> start(String name, Path workDir) {
        return doStart(name, null, workDir, defaultServiceClassLoader());
    }

    /** {@inheritDoc} */
    @Override
    public void stop(String name) {
        readyForInitNodes.remove(name);

        nodes.computeIfPresent(name, (nodeName, node) -> {
            node.stop();

            return null;
        });
    }

    @Override
    public void init(String name, Collection<String> metaStorageNodeNames) {
        init(name, metaStorageNodeNames, List.of());
    }

    @Override
    public void init(String name, Collection<String> metaStorageNodeNames, Collection<String> cmgNodeNames) {
        IgniteImpl node = readyForInitNodes.get(name);

        if (node == null) {
            throw new IgniteException("Node \"" + name + "\" has not been started");
        }

        try {
            node.init(metaStorageNodeNames, cmgNodeNames);
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
        String ver = IgniteProperties.get(VER_KEY);

        String banner = String.join("\n", BANNER);

        LOG.info(() -> IgniteStringFormatter.format("{}\n" + " ".repeat(22) + "Apache Ignite ver. {}\n", banner, ver), null);
    }
}
