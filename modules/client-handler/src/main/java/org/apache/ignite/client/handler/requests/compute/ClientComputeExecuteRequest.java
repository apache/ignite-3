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

import static org.apache.ignite.client.handler.requests.compute.ClientComputeGetStatusRequest.packJobStatus;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import org.apache.ignite.client.handler.NotificationSender;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobExecutionOptions;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.compute.IgniteComputeInternal;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;

/**
 * Compute execute request.
 */
public class ClientComputeExecuteRequest {
    /**
     * Processes the request.
     *
     * @param in Unpacker.
     * @param out Packer.
     * @param compute Compute.
     * @param cluster Cluster.
     * @param notificationSender Notification sender.
     * @return Future.
     */
    public static CompletableFuture<Void> process(
            ClientMessageUnpacker in,
            ClientMessagePacker out,
            IgniteComputeInternal compute,
            ClusterService cluster,
            NotificationSender notificationSender
    ) {
        Set<ClusterNode> candidates = unpackCandidateNodes(in, cluster);

        List<DeploymentUnit> deploymentUnits = unpackDeploymentUnits(in);
        String jobClassName = in.unpackString();
        JobExecutionOptions options = JobExecutionOptions.builder().priority(in.unpackInt()).maxRetries(in.unpackInt()).build();
        Object[] args = unpackArgs(in);

        ClusterNode localNode = cluster.topologyService().localMember();
        ClusterNode targetNode = selectTargetNode(candidates, localNode);
        candidates.remove(targetNode);

        JobExecution<Object> execution = compute.executeAsyncWithFailover(targetNode, candidates,
                deploymentUnits, jobClassName, options, args);
        sendResultAndStatus(execution, notificationSender);
        return execution.idAsync().thenAccept(out::packUuid);
    }

    private static Set<ClusterNode> unpackCandidateNodes(ClientMessageUnpacker in, ClusterService cluster) {
        Set<String> candidateIds = Arrays.stream(in.unpackObjectArrayFromBinaryTuple())
                .map(String.class::cast)
                .collect(Collectors.toSet());

        Set<ClusterNode> candidates = new HashSet<>();
        List<String> notFoundNodes = new ArrayList<>();
        candidateIds.forEach(id -> {
            ClusterNode node = cluster.topologyService().getByConsistentId(id);
            if (node == null) {
                notFoundNodes.add(id);
            } else {
                candidates.add(node);
            }
        });

        if (!notFoundNodes.isEmpty()) {
            throw new IgniteException("Specified nodes are not present in the cluster: " + notFoundNodes);
        }
        return candidates;
    }

    /**
     * Selects a random node from the set of candidates, preferably not a local node.
     *
     * @param candidates Set of candidate nodes.
     * @param localNode Local node.
     *
     * @return Target node to run a job on.
     */
    private static ClusterNode selectTargetNode(Set<ClusterNode> candidates, ClusterNode localNode) {
        if (candidates.size() == 1) {
            return candidates.iterator().next();
        }

        // Since there are more than one candidate, we can safely exclude local node here.
        // It will still be used as a failover candidate, if present.
        Set<ClusterNode> nodes = new HashSet<>(candidates);
        nodes.remove(localNode);

        if (nodes.size() == 1) {
            return nodes.iterator().next();
        }

        int nodesToSkip = ThreadLocalRandom.current().nextInt(nodes.size());

        Iterator<ClusterNode> iterator = nodes.iterator();
        for (int i = 0; i < nodesToSkip; i++) {
            iterator.next();
        }

        return iterator.next();

    }

    static void sendResultAndStatus(JobExecution<Object> execution, NotificationSender notificationSender) {
        execution.resultAsync().whenComplete((val, err) ->
                execution.statusAsync().whenComplete((status, errStatus) ->
                        notificationSender.sendNotification(w -> {
                            w.packObjectAsBinaryTuple(val);
                            packJobStatus(w, status);
                        }, err)));
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
