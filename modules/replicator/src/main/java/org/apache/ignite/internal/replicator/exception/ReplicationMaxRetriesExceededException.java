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

package org.apache.ignite.internal.replicator.exception;

import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.lang.ErrorGroups.Replicator;

/**
 * The exception is thrown when an amount of replication retries exceeds the limit.
 */
public class ReplicationMaxRetriesExceededException extends ReplicationException {
    /**
     * The constructor.
     *
     * @param replicaGrpId Replication group id.
     * @param limit Maximum possible amount of retries.
     */
    public ReplicationMaxRetriesExceededException(ReplicationGroupId replicaGrpId, int limit) {
        super(Replicator.REPLICA_COMMON_ERR, IgniteStringFormatter.format(
                "Replication retries exceeds the limit [replicaGrpId={}, limit={}]", replicaGrpId, limit));
    }
}
