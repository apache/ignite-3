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

import java.util.UUID;

/**
 * This exception is thrown when replica service is not able to wait for replica on the local node to handle a request.
 */
public class AwaitReplicaTimeoutException extends ReplicationTimeoutException {
    private static final long serialVersionUID = -2089762711985049755L;

    /**
     * The constructor is used for creating an exception instance that is thrown from a server processing the replica request.
     *
     * @param traceId Trace id.
     * @param code    Error code.
     * @param message Error message.
     * @param cause   Cause exception.
     */
    public AwaitReplicaTimeoutException(UUID traceId, int code, String message, Throwable cause) {
        super(traceId, code, message, cause);
    }
}
