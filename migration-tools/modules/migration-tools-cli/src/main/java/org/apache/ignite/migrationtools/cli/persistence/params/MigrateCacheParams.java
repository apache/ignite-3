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
import org.jetbrains.annotations.Nullable;
import picocli.CommandLine;

/** MigrateCacheParams. */
public class MigrateCacheParams {
    @CommandLine.Parameters(paramLabel = "cacheName")
    private String cacheName;

    @CommandLine.Parameters(paramLabel = "urls", arity = "1..*", defaultValue = "127.0.0.1", description = "URLs to connect to the cluster")
    private String[] addresses;

    @CommandLine.Option(names = "--mode", description = "Mapping error handling policy: ${COMPLETION-CANDIDATES}", defaultValue = "ABORT")
    private MigrationMode migrationMode;

    @CommandLine.Option(names = {"--rate-limiter"}, defaultValue = "-1", description = "Limits the number of migrated records per second."
            + " Uses a very basic rate limiter implementation, may be prone to bursts.")
    private int rateLimiter;

    @CommandLine.Option(names = {"--no-save-progress"}, description = "Disables saving a progress file at the end of the run.")
    private boolean saveProgressFileDisabled;

    @Nullable
    @CommandLine.Option(names = {"--resume-from"}, description = "Resumes the migration based on the progress file provided.")
    private Path progressFileToRead;

    private MigrateCacheParams() {
        // Intentionally left blank.
    }

    /**
     * Constructor.
     *
     * @param cacheName Cache Name.
     * @param addresses Address.
     * @param migrationMode Migration Mode.
     * @param rateLimiter Rate Limiter
     * @param saveProgressFileDisabled Save Progress File Enabled.
     * @param progressFileToRead Progress file to read.
     */
    public MigrateCacheParams(
            String cacheName,
            String[] addresses,
            MigrationMode migrationMode,
            int rateLimiter,
            boolean saveProgressFileDisabled,
            @Nullable Path progressFileToRead) {
        this.cacheName = cacheName;
        this.addresses = addresses;
        this.migrationMode = migrationMode;
        this.rateLimiter = rateLimiter;
        this.saveProgressFileDisabled = saveProgressFileDisabled;
        this.progressFileToRead = progressFileToRead;
    }

    public String cacheName() {
        return cacheName;
    }

    public String[] addresses() {
        return addresses;
    }

    public MigrationMode migrationMode() {
        return migrationMode;
    }

    public int rateLimiter() {
        return rateLimiter;
    }

    public boolean saveProgressFileDisabled() {
        return saveProgressFileDisabled;
    }

    public Path progressFileToRead() {
        return progressFileToRead;
    }
}
