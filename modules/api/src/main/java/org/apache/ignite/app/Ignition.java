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

package org.apache.ignite.app;

import java.io.InputStream;
import java.nio.file.Path;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Entry point for handling the grid lifecycle.
 */
@SuppressWarnings("UnnecessaryInterfaceModifier")
public interface Ignition {
    /**
     * Starts an Ignite node with an optional bootstrap configuration from a HOCON file.
     *
     * @param name Name of the node. Must not be {@code null}.
     * @param configPath Path to the node configuration in the HOCON format. Can be {@code null}.
     * @param workDir Work directory for the started node. Must not be {@code null}.
     * @return Started Ignite node.
     */
    public Ignite start(@NotNull String name, @Nullable Path configPath, @NotNull Path workDir);

    /**
     * Starts an Ignite node with an optional bootstrap configuration from an input stream with HOCON configs.
     *
     * @param name Name of the node. Must not be {@code null}.
     * @param config Input stream from the node configuration in HOCON format. Can be {@code null}.
     * @param workDir Work directory for the started node. Must not be {@code null}.
     * @return Started Ignite node.
     */
    public Ignite start(@NotNull String name, @Nullable InputStream config, @NotNull Path workDir);

    /**
     * Starts an Ignite node with the default configuration.
     *
     * @param name Name of the node. Must not be {@code null}.
     * @param workDir Work directory for the started node. Must not be {@code null}.
     * @return Started Ignite node.
     */
    public Ignite start(@NotNull String name, @NotNull Path workDir);

    /**
     * Stops the node with given {@code name}.
     * It's possible to stop both already started node or node that is currently starting.
     * Has no effect if node with specified name doesn't exist.
     *
     * @param name Node name to stop.
     * @throws IllegalArgumentException if null is specified instead of node name.
     */
    public void stop(@NotNull String name);
}
