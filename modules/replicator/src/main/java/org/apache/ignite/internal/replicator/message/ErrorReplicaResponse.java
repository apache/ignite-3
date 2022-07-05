/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.replicator.message;

import java.util.UUID;
import org.apache.ignite.network.annotations.Transferable;

/**
 * The message can be sent as response to any {@link ReplicaRequest} if during the request executing on replica an unhandled exception
 * happened.
 */
@Transferable(ReplicaMessageGroup.ERROR_REPLICA_RESPONSE)
public interface ErrorReplicaResponse extends ReplicaResponse {
    /**
     * Gets an error code. All code are available in {@link org.apache.ignite.lang.ErrorGroups.Replicator}
     *
     * @return Error code.
     */
    int errorCode();

    /**
     * Gets an exception name.
     *
     * @return Fully qualified error name.
     */
    String errorClassName();

    /**
     * Gets an error text message.
     *
     * @return Message.
     */
    String errorMessage();

    /**
     * Get a trace id by that determines a trace in the remote node.
     *
     * @return Trace id.
     */
    UUID errorTraceId();

    /**
     * If the passing stack trace is switched on, the method returns a stack, otherwise the method returns a {@nill}.
     *
     * @return Stack trace as a string or {@code null}.
     */
    String errorStackTrace();
}
