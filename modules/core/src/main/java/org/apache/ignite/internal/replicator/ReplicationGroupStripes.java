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

package org.apache.ignite.internal.replicator;

import java.util.concurrent.ExecutorService;
import org.apache.ignite.internal.thread.StripedThreadPoolExecutor;

/**
 * Logic used to calculate a stripe (or its index) to use to handle a replication request by its replication group ID.
 */
public class ReplicationGroupStripes {
    /**
     * Returns an executor that will execute requests belonging to the given replication group ID.
     *
     * @param groupId ID of the group.
     * @param stripedExecutor Striped executor from which to take a stripe.
     */
    public static ExecutorService stripeFor(ReplicationGroupId groupId, StripedThreadPoolExecutor stripedExecutor) {
        return stripedExecutor.stripeExecutor(groupId.hashCode());
    }
}
