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

package org.apache.ignite.internal.jdbc.proto.event;

import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.jetbrains.annotations.Nullable;

/**
 * Result of commit/rollback command.
 */
public class JdbcFinishTxResult extends Response {
    /** Observable timestamp used only on server side. */
    @SuppressWarnings("TransientFieldInNonSerializableClass")
    private final transient @Nullable HybridTimestamp observableTime;

    /**
     * Default constructor is used for deserialization.
     */
    public JdbcFinishTxResult() {
        this.observableTime = null;
    }

    /**
     * Constructor.
     */
    public JdbcFinishTxResult(@Nullable HybridTimestamp observableTime) {
        this.observableTime = observableTime;
    }

    /**
     * Constructor.
     *
     * @param status Status code.
     * @param err    Error message.
     */
    public JdbcFinishTxResult(int status, String err) {
        super(status, err);

        this.observableTime = null;
    }

    /**
     * Returns transaction observable time.
     */
    public @Nullable HybridTimestamp observableTime() {
        return observableTime;
    }
}
