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
