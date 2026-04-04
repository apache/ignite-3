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

package org.apache.ignite.internal.raft;

import static org.apache.ignite.lang.ErrorGroups.Replicator.GROUP_OVERLOADED_ERR;

import org.apache.ignite.lang.IgniteException;

/**
 * Exception thrown by a Raft client when a peer is overloaded and wouldn't be able to process the request at the moment.
 */
public class GroupOverloadedException extends IgniteException {
    private static final long serialVersionUID = 2369606399873450609L;

    /**
     * Constructor.
     *
     * @param groupId Replication group id.
     */
    public GroupOverloadedException(String groupId) {
        super(GROUP_OVERLOADED_ERR, "Group is overloaded, the request may be retried later [groupId=" + groupId + "].");
    }
}
