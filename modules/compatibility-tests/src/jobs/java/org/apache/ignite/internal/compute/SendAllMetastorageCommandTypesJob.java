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

package org.apache.ignite.internal.compute;

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.dsl.Operations.noop;
import static org.apache.ignite.internal.metastorage.dsl.Operations.ops;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.metastorage.dsl.Statements.iif;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.metastorage.impl.MetaStorageManagerImpl;
import org.apache.ignite.internal.wrapper.Wrappers;

/** A job that runs different MetastorageWriteCommands. */
// TODO IGNITE-26874 Add a check that all write commands are covered.
public class SendAllMetastorageCommandTypesJob implements ComputeJob<String, Void> {
    @Override
    public CompletableFuture<Void> executeAsync(JobExecutionContext context, String arg) {
        IgniteImpl igniteImpl = Wrappers.unwrap(context.ignite(), IgniteImpl.class);

        byte[] value = "value".getBytes(StandardCharsets.UTF_8);

        MetaStorageManagerImpl metastorage = (MetaStorageManagerImpl) igniteImpl.metaStorageManager();

        CompletableFuture<Void> sendCommandsFuture = allOf(
                metastorage.put(ByteArray.fromString("put"), value),
                metastorage.putAll(Map.of(ByteArray.fromString("putAll"), value)),
                metastorage.remove(ByteArray.fromString("remove")),
                metastorage.removeAll(Set.of(ByteArray.fromString("removeAll"))),
                metastorage.removeByPrefix(ByteArray.fromString("removeByPrefix")),
                metastorage.invoke(
                        notExists(ByteArray.fromString("invoke")),
                        put(ByteArray.fromString("invoke"), value),
                        noop()
                ),
                metastorage.invoke(
                        iif(
                                notExists(ByteArray.fromString("iff")),
                                ops(put(ByteArray.fromString("iff"), value)).yield(),
                                ops().yield()
                        )
                ),
                metastorage.evictIdempotentCommandsCache(HybridTimestamp.MAX_VALUE)
        );

        return sendCommandsFuture
                // Send compaction command after other commands have been applied to guarantee a non-zero applied index.
                .thenCompose(v -> sendCompactionCommand(metastorage))
                .thenCompose(v -> metastorage.storage().flush());
    }

    private static CompletableFuture<Void> sendCompactionCommand(MetaStorageManagerImpl metastorage) {
        try {
            Method sendCompactionCommand = metastorage.getClass().getDeclaredMethod("sendCompactionCommand", long.class);
            sendCompactionCommand.setAccessible(true);

            return (CompletableFuture<Void>) sendCompactionCommand.invoke(metastorage, 0);
        } catch (Exception e) {
            return failedFuture(e);
        }
    }
}
