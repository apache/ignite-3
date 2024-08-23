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

package org.apache.ignite.internal.disaster.system;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.disaster.system.message.ResetClusterMessage;

/**
 * Manages disaster recovery of system groups, namely the Cluster Management Group (CMG) and the Metastorage group (MG).
 */
public interface SystemDisasterRecoveryManager {
    /**
     * Saves cluster name to the Vault if not already saved.
     *
     * @param clusterName Name of the cluster to save.
     */
    void saveClusterName(String clusterName);

    /**
     * Marks this node as initialized.
     */
    void markNodeInitialized();

    /**
     * Initiates cluster reset.
     *
     * @param proposedCmgConsistentIds Names of the nodes that will be the new CMG nodes.
     * @return Future completing with the result of the operation ({@link ResetClusterMessage} in case of error related to reset logic).
     */
    CompletableFuture<Void> resetCluster(List<String> proposedCmgConsistentIds);
}
