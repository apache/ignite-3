/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.migrationtools.cli.persistence.calls;

import org.apache.ignite.migrationtools.cli.persistence.params.MigrateCacheParams;
import org.apache.ignite.migrationtools.cli.persistence.params.PersistenceParams;
import org.apache.ignite.migrationtools.cli.persistence.params.RetrieableMigrateCacheParams;
import org.apache.ignite3.internal.cli.core.call.Call;
import org.apache.ignite3.internal.cli.core.call.CallInput;
import org.apache.ignite3.internal.cli.core.call.CallOutput;
import org.apache.ignite3.internal.cli.core.call.DefaultCallOutput;
import org.apache.ignite3.internal.cli.core.exception.IgniteCliException;
import org.apache.ignite3.internal.cli.logger.CliLoggers;
import org.apache.ignite3.internal.logger.IgniteLogger;

/** Call for the Retrieable Migrate Cache Command. */
public class RetriableMigrateCacheCall implements Call<RetriableMigrateCacheCall.Input, MigrateCacheCall.Ouput> {
    private static final IgniteLogger LOGGER = CliLoggers.forClass(MigrateCacheCall.class);

    private final MigrateCacheCall migrateCacheCall;

    public RetriableMigrateCacheCall(MigrateCacheCall migrateCacheCall) {
        this.migrateCacheCall = migrateCacheCall;
    }

    @Override
    public CallOutput<MigrateCacheCall.Ouput> execute(Input i) {
        int retryLimit = i.retryParms().retryLimit();
        int retryBackoffSeconds = i.retryParms().retryBackoffSeconds();

        if (retryLimit < 0) {
            LOGGER.error("retryLimit must be >= 0 times, but was {}", retryLimit);
            return DefaultCallOutput.failure(new IgniteCliException("retryLimit must be >= 0 times, but was " + retryLimit));
        }

        if (retryBackoffSeconds < 0) {
            LOGGER.error("retryBackoff must be >= 0 seconds, but was {}", retryBackoffSeconds);
            return DefaultCallOutput.failure(new IgniteCliException("retryBackoff must be >= 0 seconds, but was " + retryBackoffSeconds));
        }

        if (retryLimit > 0 && i.migrateCacheParams().saveProgressFileDisabled()) {
            LOGGER.error("--no-save-progress cannot be used with --retryLimit > 0");
            return DefaultCallOutput.failure(new IgniteCliException("--no-save-progress cannot be used with --retryLimit > 0"));
        }

        MigrateCacheParams migrateCacheParams = i.migrateCacheParams();

        int atempt = 0;
        while (atempt < retryLimit) {
            CallOutput<MigrateCacheCall.Ouput> status = migrateCacheCall.execute(
                    new MigrateCacheCall.Input(i.persistenceParams(), migrateCacheParams));
            if (status.errorCause() == null || !status.errorCause().getMessage().equals("Error while migrating persistence folder")) {
                return status;
            }

            LOGGER.warn("Cache migration attempt {} failed. Will retry in {}s. {} attempts remaining.",
                    atempt, retryBackoffSeconds, retryLimit - atempt);

            if (retryBackoffSeconds > 0) {
                try {
                    Thread.sleep(retryBackoffSeconds * 1_000L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOGGER.warn("Interrupted during retry attempt backoff.");
                    return DefaultCallOutput.failure(new IgniteCliException("Interrupted during retry attempt backoff.", e));
                }
            }

            atempt++;

            // make the next call resumeFrom the progressFile of the previous attempt.
            migrateCacheParams = new MigrateCacheParams(
                    migrateCacheParams.cacheName(),
                    migrateCacheParams.addresses(),
                    migrateCacheParams.migrationMode(),
                    migrateCacheParams.rateLimiter(),
                    migrateCacheParams.saveProgressFileDisabled(),
                    status.body().getProgressFilePath()
            );
        }

        CallOutput<MigrateCacheCall.Ouput> lastStatus = migrateCacheCall.execute(
                new MigrateCacheCall.Input(i.persistenceParams(), migrateCacheParams));

        return lastStatus;
    }

    /** Inputs. */
    public static class Input implements CallInput {
        private final PersistenceParams persistenceParams;

        private final MigrateCacheParams migrateCacheParams;

        private final RetrieableMigrateCacheParams retryParms;

        /**
         * Constructor.
         *
         * @param persistenceParams Persistence Params.
         * @param migrateCacheParams Migrate cache parameters.
         * @param retryParms Retry parameters.
         */
        public Input(PersistenceParams persistenceParams, MigrateCacheParams migrateCacheParams, RetrieableMigrateCacheParams retryParms) {
            this.persistenceParams = persistenceParams;
            this.migrateCacheParams = migrateCacheParams;
            this.retryParms = retryParms;
        }

        public PersistenceParams persistenceParams() {
            return persistenceParams;
        }

        public MigrateCacheParams migrateCacheParams() {
            return migrateCacheParams;
        }

        public RetrieableMigrateCacheParams retryParms() {
            return retryParms;
        }
    }

}
