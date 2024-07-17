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

package org.apache.ignite.internal.table;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.deployment.DeploymentUnit;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.ReceiverDescriptor;
import org.jetbrains.annotations.Nullable;

/**
 * Runs data streamer receiver on specified node with failover, class loading, scheduling.
 */
public interface StreamerReceiverRunner {
    /**
     * Runs streamer receiver.
     *
     * @param receiver Receiver.
     * @param receiverArg Argument.
     * @param items Data items.
     * @param node Target node.
     * @param deploymentUnits Deployment units.
     * @return Receiver results.
     * @param <A> Argument type.
     * @param <I> Data item type.
     * @param <R> Result type.
     */
    <A, I, R> CompletableFuture<Collection<R>> runReceiverAsync(
            ReceiverDescriptor<A> receiver,
            @Nullable A receiverArg,
            Collection<I> items,
            ClusterNode node,
            List<DeploymentUnit> deploymentUnits);

    /**
     * Runs streamer receiver in serialized mode (takes serialized payload, returns serialized results).
     * This method avoids intermediate deserialization/serialization on the client request handler side
     * and passes serialized data directly to/from client.
     *
     * @param payload Serialized payload.
     * @param node Target node.
     * @param deploymentUnits Deployment units.
     * @return Serialized receiver results.
     */
    CompletableFuture<byte[]> runReceiverAsync(byte[] payload, ClusterNode node, List<DeploymentUnit> deploymentUnits);
}
