/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.migrationtools.cli.persistence.params;

import java.nio.file.Path;
import picocli.CommandLine;

/** Persistence Command parameters. */
public class PersistenceParams {
    @CommandLine.Parameters(paramLabel = "work-dir", description = "Work Directory of one or many Ignite 2 or Gridgain 8 nodes")
    private Path workDir;
    @CommandLine.Parameters(paramLabel = "consistent-id", description = "Consistent ID of the Ignite 2 or Gridgain 8 node")
    private String nodeConsistentId;
    @CommandLine.Parameters(paramLabel = "config-file", description = "Ignite 2 or Gridgain 8 Configuration XML")
    private Path configFile;

    public Path workDir() {
        return workDir;
    }

    public String nodeConsistentId() {
        return nodeConsistentId;
    }

    public Path configFile() {
        return configFile;
    }
}
