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

package org.apache.ignite.internal.network.recovery.message;

/**
 * Reason for handshake rejection.
 */
public enum HandshakeRejectionReason {
    /** The sender is stopping. */
    STOPPING,

    /**
     * An attempt to establish a connection to itself. This should never happen and indicates a programming error.
     */
    LOOP,

    /**
     * The sender has detected that the counterpart launch ID is stale (was earlier used to establish a connection).
     * After this is received it makes no sense to retry connections with same node identity (launch ID must be changed
     * to make a retry).
     */
    STALE_LAUNCH_ID,

    /** The sender has detected a clinch and decided to terminate this handshake in favor of the competitor. */
    CLINCH,

    /**
     * Cluster ID of the sender does not match the cluster ID of the counterpart.
     */
    CLUSTER_ID_MISMATCH,

    /**
     * Product (Ignite or a derivative product) of the sender does not match the product of the counterpart.
     */
    PRODUCT_MISMATCH,

    /**
     * Version of the sender product does not match the version of the counterpart.
     */
    VERSION_MISMATCH;

    /**
     * Returns {@code true} iff the rejection should be logged at a WARN level.
     */
    public boolean logAsWarn() {
        return this == LOOP
                || this == STALE_LAUNCH_ID
                || this == CLUSTER_ID_MISMATCH
                || this == PRODUCT_MISMATCH
                || this == VERSION_MISMATCH;
    }
}
