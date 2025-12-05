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

package org.apache.ignite.internal.table.distributed.disaster;

import static org.apache.ignite.internal.table.distributed.disaster.MetaStorageKeys.COMPLETED_BYTES;
import static org.apache.ignite.internal.table.distributed.disaster.MetaStorageKeys.IN_PROGRESS_BYTES;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.internal.util.StringUtils.toStringWithoutPrefix;
import static org.apache.ignite.lang.ErrorGroups.DisasterRecovery.LOCAL_NODE_ERR;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyEventListener;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.EntryEvent;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.table.distributed.disaster.exceptions.DisasterRecoveryException;
import org.apache.ignite.internal.table.distributed.disaster.exceptions.LocalProcessingDisasterRecoveryException;
import org.apache.ignite.internal.util.CollectionUtils;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;

/** Watch for tracking MULTI_NODE disaster recovery process. */
class RecoveryWatch implements WatchListener, LogicalTopologyEventListener {
    private static final long ACKNOWLEDGE_TIMEOUT_SECONDS = 10;

    private final CompletableFuture<Void> operationFuture;

    private final ByteArray prefix;

    private final IgniteSpinBusyLock busyLock;

    // Updates are processes in one thread, no need for concurrent set.
    private final Set<String> expectingNodeNames;

    private final Set<String> acknowledgedNodes = new HashSet<>();

    private final CompletableFuture<Void> nodesStartedFuture;

    RecoveryWatch(IgniteSpinBusyLock busyLock, Set<String> expectingNodeNames, ByteArray prefix, CompletableFuture<Void> operationFuture) {
        this.busyLock = busyLock;
        this.prefix = prefix;
        this.operationFuture = operationFuture;
        this.expectingNodeNames = new HashSet<>(expectingNodeNames);

        nodesStartedFuture = new CompletableFuture<Void>()
                .orTimeout(ACKNOWLEDGE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .whenComplete((res, ex) -> {
                    if (ex != null) {
                        operationFuture.completeExceptionally(new DisasterRecoveryException(
                                LOCAL_NODE_ERR,
                                "Not all nodes acknowledged the recovery process start in time: "
                                        + CollectionUtils.difference(expectingNodeNames, acknowledgedNodes)
                        ));
                    }
                });
    }

    @Override
    public CompletableFuture<Void> onUpdate(WatchEvent event) {
        return inBusyLock(busyLock, () -> {
            for (EntryEvent entryEvent : event.entryEvents()) {
                Entry entry = entryEvent.newEntry();

                String nodeName = extractNodeName(entry.key());

                if (Arrays.equals(IN_PROGRESS_BYTES, entry.value())) {
                    acknowledgedNodes.add(nodeName);
                    if (acknowledgedNodes.size() == expectingNodeNames.size()) {
                        nodesStartedFuture.complete(null);
                    }
                } else if (Arrays.equals(COMPLETED_BYTES, entry.value())) {
                    expectingNodeNames.remove(nodeName);
                    if (expectingNodeNames.isEmpty()) {
                        operationFuture.complete(null);
                    }
                } else if (entry.value() != null) {
                    operationFuture.completeExceptionally(new LocalProcessingDisasterRecoveryException(
                            new String(entry.value()),
                            nodeName
                    ));
                }
            }

            return nullCompletedFuture();
        });
    }

    @Override
    public void onNodeLeft(LogicalNode leftNode, LogicalTopologySnapshot newTopology) {
        if (expectingNodeNames.contains(leftNode.name())) {
            operationFuture.completeExceptionally(new DisasterRecoveryException(
                    LOCAL_NODE_ERR,
                    "Node " + leftNode.name() + " has left the cluster during recovery process."
            ));
        }
    }

    private String extractNodeName(byte[] key) {
        return toStringWithoutPrefix(key, prefix.length());
    }
}
