/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
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
        description = "Commands to interact with an Ignite 2 or Gridgain 8 node work directory",
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
