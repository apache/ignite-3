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

package org.apache.ignite.migrationtools.cli.persistence.commands;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.migrationtools.cli.persistence.params.PersistenceParams;
import org.apache.ignite.migrationtools.config.Ignite2ConfigurationUtils;
import org.apache.ignite.migrationtools.persistence.Ignite2PersistenceTools;
import org.apache.ignite.migrationtools.persistence.MigrationKernalContext;
import org.apache.ignite3.internal.cli.commands.BaseCommand;
import org.apache.ignite3.internal.cli.logger.CliLoggers;
import org.apache.ignite3.internal.logger.IgniteLogger;
import org.jetbrains.annotations.Nullable;
import picocli.CommandLine;

/** Base for all the Persistence commands. */
@CommandLine.Command(
        name = "persistent-data",
        description = "Commands to interact with an Ignite 2 node work directory",
        subcommands = {
                ListCachesCmd.class,
                MigrateCacheCmd.class
        })
public class PersistenceBaseCmd extends BaseCommand {
    private static final IgniteLogger LOGGER = CliLoggers.forClass(PersistenceBaseCmd.class);

    @CommandLine.Mixin
    private PersistenceParams params;

    /**
     * Creates a valid Ignite configuration.
     *
     * @param params The commands param.
     * @return A valid {@link IgniteConfiguration}, or {@code null} if it doesn't exist.
     */
    @Nullable
    public static IgniteConfiguration createValidIgniteCfg(PersistenceParams params) {
        var cfg = Ignite2ConfigurationUtils.loadIgnite2Configuration(params.configFile().toFile(), true);
        // Override the configuration work directory, allows running on a different base folder than the original node.
        cfg.setWorkDirectory(params.workDir().toString());
        var errors = MigrationKernalContext.validateConfigurations(cfg);
        if (!errors.isEmpty()) {
            LOGGER.error("Could not load all the required configuration modules: %s", errors);
            return null;
        }

        return cfg;
    }

    /**
     * Creates and starts the {@link MigrationKernalContext}s for the provided params and configurations.
     *
     * @param params The command params.
     * @param cfg The Ignite 2 cluster configuration.
     * @return A list with the started {@link MigrationKernalContext}s.
     * @throws IgniteCheckedException if any error occurs.
     */
    public static List<MigrationKernalContext> createAndStartMigrationContext(PersistenceParams params, IgniteConfiguration cfg)
            throws IgniteCheckedException {
        List<Ignite2PersistenceTools.NodeFolderDescriptor> nodeFolders = Ignite2PersistenceTools.nodeFolderCandidates(cfg).stream()
                .filter(c -> params.nodeConsistentId().equals(c.consistentId().toString()))
                .collect(Collectors.toList());

        List<MigrationKernalContext> nodeContexts = new ArrayList<>(nodeFolders.size());
        for (Ignite2PersistenceTools.NodeFolderDescriptor nodeFolder : nodeFolders) {
            MigrationKernalContext ctx = new MigrationKernalContext(cfg, nodeFolder.subFolderFile(), nodeFolder.consistentId());
            nodeContexts.add(ctx);

            ctx.start();
        }

        return nodeContexts;
    }

    public PersistenceParams params() {
        return params;
    }
}
