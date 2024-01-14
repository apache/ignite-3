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

package org.apache.ignite.client.handler.requests.compute;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.client.handler.NotificationSender;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.network.ClusterService;
import org.jetbrains.annotations.Nullable;

/**
 * Compute execute request.
 */
public class ClientComputeExecuteRequest {
    /**
     * Processes the request.
     *
     * @param in                 Unpacker.
     * @param compute            Compute.
     * @param cluster            Cluster.
     * @param notificationSender Notification sender.
     * @return Future.
     */
    public static @Nullable CompletableFuture<Void> process(
            ClientMessageUnpacker in,
            IgniteCompute compute,
            ClusterService cluster,
            NotificationSender notificationSender) {
        var nodeName = in.tryUnpackNil() ? null : in.unpackString();

        var node = nodeName == null
                ? cluster.topologyService().localMember()
                : cluster.topologyService().getByConsistentId(nodeName);

        if (node == null) {
            throw new IgniteException("Specified node is not present in the cluster: " + nodeName);
        }

        List<DeploymentUnit> deploymentUnits = unpackDeploymentUnits(in);
        String jobClassName = in.unpackString();
        Object[] args = unpackArgs(in);

        compute.executeAsync(Set.of(node), deploymentUnits, jobClassName, args)
                .resultAsync()
                .whenComplete((res, err) -> notificationSender.sendNotification(w -> w.packObjectAsBinaryTuple(res), err));

        return null;
    }

    /**
     * Unpacks args.
     *
     * @param in Unpacker.
     * @return Args array.
     */
    static Object[] unpackArgs(ClientMessageUnpacker in) {
        return in.unpackObjectArrayFromBinaryTuple();
    }

    /**
     * Unpacks deployment units.
     *
     * @param in Unpacker.
     * @return Deployment units.
     */
    static List<DeploymentUnit> unpackDeploymentUnits(ClientMessageUnpacker in) {
        int size = in.tryUnpackNil() ? 0 : in.unpackInt();
        List<DeploymentUnit> res = new ArrayList<>(size);

        for (int i = 0; i < size; i++) {
            res.add(new DeploymentUnit(in.unpackString(), in.unpackString()));
        }

        return res;
    }
}
