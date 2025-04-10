/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.migrationtools.cli.persistence.calls;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.migrationtools.cli.persistence.commands.PersistenceBaseCmd;
import org.apache.ignite.migrationtools.cli.persistence.params.PersistenceParams;
import org.apache.ignite.migrationtools.persistence.Ignite2PersistentCacheTools;
import org.apache.ignite.migrationtools.persistence.MigrationKernalContext;
import org.apache.ignite3.internal.cli.core.call.Call;
import org.apache.ignite3.internal.cli.core.call.CallInput;
import org.apache.ignite3.internal.cli.core.call.DefaultCallOutput;
import org.apache.ignite3.internal.cli.core.exception.IgniteCliException;
import org.apache.ignite3.internal.cli.logger.CliLoggers;
import org.apache.ignite3.internal.logger.IgniteLogger;
import org.jetbrains.annotations.Nullable;

/** Call to the List Caches Command. */
public class ListCachesCall implements Call<ListCachesCall.Input, List<ListCachesCall.Output>> {
    private static final IgniteLogger LOGGER = CliLoggers.forClass(ListCachesCall.class);

    @Override
    public DefaultCallOutput<List<Output>> execute(Input i) {
        List<MigrationKernalContext> persistentCtx = Collections.emptyList();
        try {
            @Nullable IgniteConfiguration cfg = PersistenceBaseCmd.createValidIgniteCfg(i.persistenceParams());
            if (cfg == null) {
                return DefaultCallOutput.failure(new IgniteCliException("Unable to read ignite configuration"));
            }

            persistentCtx = PersistenceBaseCmd.createAndStartMigrationContext(i.persistenceParams(), cfg);
            if (persistentCtx.isEmpty()) {
                return DefaultCallOutput.failure(new IgniteCliException(
                        String.format("Could not find node (consistentId:%s) folder in '%s'", i.persistenceParams().nodeConsistentId(),
                                i.persistenceParams().workDir().toString())
                ));
            }

            var cacheIds = Ignite2PersistentCacheTools.persistentCaches(persistentCtx);
            LOGGER.info("Found {} caches", cacheIds.size());

            var ret = cacheIds.stream()
                    .map(e -> new Output(e.getRight(), e.getLeft()))
                    .collect(Collectors.toList());

            return DefaultCallOutput.success(ret);
        } catch (IgniteCheckedException e) {
            return DefaultCallOutput.failure(new IgniteCliException("Error while calling list-caches", e));
        } finally {
            for (var ctx : persistentCtx) {
                try {
                    ctx.stop();
                } catch (IgniteCheckedException e) {
                    LOGGER.error("Error stopping node context", e);
                }
            }
        }
    }

    /** Input. */
    public static class Input implements CallInput {
        private PersistenceParams persistenceParams;

        public Input(PersistenceParams persistenceParams) {
            this.persistenceParams = persistenceParams;
        }

        public PersistenceParams persistenceParams() {
            return persistenceParams;
        }
    }

    /** Output. */
    public static class Output {
        private final String cacheName;

        private final int cacheId;

        public Output(String cacheName, int cacheId) {
            this.cacheName = cacheName;
            this.cacheId = cacheId;
        }

        public String cacheName() {
            return cacheName;
        }

        public int cacheId() {
            return cacheId;
        }
    }
}
