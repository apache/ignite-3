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

package org.apache.ignite.internal.client;

import java.util.UUID;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.jetbrains.annotations.Nullable;

public class ClientDelayedAckException extends IgniteInternalException {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Transaction id. */
    private final UUID txId;

    /**
     * Constructor.
     *
     * @param traceId Trace ID.
     * @param code Error code.
     *
     * @param message String message.
     * @param txId Related transaction id.
     * @param cause Cause.
     */
    ClientDelayedAckException(UUID traceId, int code, @Nullable String message, UUID txId, @Nullable Throwable cause) {
        super(traceId, code, message, cause);

        this.txId = txId;
    }

    /**
     * Gets expected schema version.
     *
     * @return Expected schema version.
     */
    public UUID txId() {
        return txId;
    }
}
