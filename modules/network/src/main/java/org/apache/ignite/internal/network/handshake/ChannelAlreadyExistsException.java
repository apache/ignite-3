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

package org.apache.ignite.internal.network.handshake;

/**
 * Exception that notifies of existence of a channel with a specific consistent id during handshake.
 */
public class ChannelAlreadyExistsException extends RuntimeException {
    private static final long serialVersionUID = 0L;

    /** Consistent id of a remote node. */
    private final String consistentId;

    public ChannelAlreadyExistsException(String consistentId) {
        this.consistentId = consistentId;
    }

    /**
     * Returns consistent id of the remote node with which a channel already exists.
     */
    public String consistentId() {
        return consistentId;
    }
}
