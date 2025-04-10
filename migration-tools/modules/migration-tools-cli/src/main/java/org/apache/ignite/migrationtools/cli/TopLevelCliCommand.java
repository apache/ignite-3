/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.migrationtools.cli;

import static org.apache.ignite3.internal.cli.commands.CommandConstants.VERSION_OPTION_ORDER;
import static org.apache.ignite3.internal.cli.commands.Options.Constants.VERSION_OPTION;
import static org.apache.ignite3.internal.cli.commands.Options.Constants.VERSION_OPTION_DESC;

import org.apache.ignite.migrationtools.cli.configs.commands.ConfigurationConverterCmd;
import org.apache.ignite.migrationtools.cli.persistence.commands.PersistenceBaseCmd;
import org.apache.ignite.migrationtools.cli.sql.commands.SqlDdlGeneratorCmd;
import org.apache.ignite3.internal.cli.VersionProvider;
import org.apache.ignite3.internal.cli.commands.BaseCommand;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Top-level command that prints help and declares subcommands.
 */
@Command(name = "migration-tools",
        versionProvider = VersionProvider.class,
        description = {
                "Welcome to Ignite Migration Tools."
        },
        subcommands = {
                CommandLine.HelpCommand.class,
                ConfigurationConverterCmd.class,
                SqlDdlGeneratorCmd.class,
                PersistenceBaseCmd.class
        })
public class TopLevelCliCommand extends BaseCommand {
    @SuppressWarnings("PMD.UnusedPrivateField")
    @Option(names = VERSION_OPTION, versionHelp = true, description = VERSION_OPTION_DESC, order = VERSION_OPTION_ORDER)
    private boolean versionRequested;
}
