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

package org.apache.ignite.internal.lang;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import java.util.Objects;
import java.util.UUID;

/**
 * Remote exception stub. This exception is used to indicate an exception was thrown and logged on the remote node.
 */
public class RemoteException extends IgniteInternalException {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /**
     * Creates a new exception with the given trace id and error code.
     *
     * @param nodeName Node name, where original exception was thrown.
     * @param traceId Unique identifier of this exception.
     * @param code Full error code.
     * @param originalMessage Original error message from remote node.
     */
    public RemoteException(String nodeName, UUID traceId, int code, String originalMessage) {
        this(traceId, code, format("Remote exception: node={}, originalMessage={}", Objects.requireNonNull(nodeName), originalMessage));
    }

    /**
     * Creates a new exception with the given trace id, error code and detail message.
     *
     * @param traceId Unique identifier of this exception.
     * @param code Full error code.
     * @param message Detail message.
     */
    protected RemoteException(UUID traceId, int code, String message) {
        super(traceId, code, message, null);
    }
}
