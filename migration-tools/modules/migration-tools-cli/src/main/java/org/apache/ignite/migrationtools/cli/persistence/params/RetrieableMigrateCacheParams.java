/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.migrationtools.cli.persistence.params;

import picocli.CommandLine;

/** Parameter for the Retriable Migrate Cache command. */
public class RetrieableMigrateCacheParams {
    @CommandLine.Option(names = {"--retryLimit"}, defaultValue = "0",
            description = "Retries the migration up to N times on retrievable errors. 0 (Default) does not retry."
                    + " Implies save progress is not disabled.")
    private int retryLimit;

    @CommandLine.Option(names = {"--retryBackoff"}, defaultValue = "0",
            description = "Waits N seconds before retry the next attempt at migration the cache. Default: 0 (retry immediately).")
    private int retryBackoffSeconds;

    private RetrieableMigrateCacheParams() {
        // Intentionally left blank.
    }

    public RetrieableMigrateCacheParams(int retryLimit, int retryBackoffSeconds) {
        this.retryLimit = retryLimit;
        this.retryBackoffSeconds = retryBackoffSeconds;
    }

    public int retryLimit() {
        return retryLimit;
    }

    public int retryBackoffSeconds() {
        return retryBackoffSeconds;
    }
}
