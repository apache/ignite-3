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

package org.apache.ignite.internal.partition.replicator;

import org.apache.ignite.internal.event.Event;

/**
 * Events produced by {@link PartitionReplicaLifecycleManager}.
 */
public enum LocalPartitionReplicaEvent implements Event {
    /**
     * Fired before a partition replica is started.
     */
    BEFORE_REPLICA_STARTED,

    /**
     * Fired before a partition replica is stopped.
     */
    BEFORE_REPLICA_STOPPED,

    /**
     * Fired after a partition replica has been stopped (but not destroyed).
     */
    AFTER_REPLICA_STOPPED,

    /**
     * Fired before a partition replica is stopped and destroyed ({@link #BEFORE_REPLICA_STOPPED} will not be produced in this case).
     */
    BEFORE_REPLICA_DESTROYED,

    /**
     * Fired after a partition replica has been stopped and destroyed ({@link #AFTER_REPLICA_STOPPED} will not be produced in this case).
     */
    AFTER_REPLICA_DESTROYED
}
