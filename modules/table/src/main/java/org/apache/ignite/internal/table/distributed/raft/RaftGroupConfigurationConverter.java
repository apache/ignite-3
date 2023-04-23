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

package org.apache.ignite.internal.table.distributed.raft;

import org.apache.ignite.internal.util.ByteUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Used to convert {@link RaftGroupConfiguration} to/from bytes.
 */
public class RaftGroupConfigurationConverter {
    /**
     * Parses the byte representation of a {@link RaftGroupConfiguration}.
     *
     * @param bytes Byte representation (might be null).
     * @return Parsed value ({@code null} if the input is {@code null}).
     */
    @Nullable
    public RaftGroupConfiguration fromBytes(byte @Nullable [] bytes) {
        if (bytes == null) {
            return null;
        }

        return ByteUtils.fromBytes(bytes);
    }

    /**
     * Converts the given configuration to its byte representation.
     *
     * @param configuration Config to convert.
     * @return Byte representation.
     */
    public byte[] toBytes(RaftGroupConfiguration configuration) {
        return ByteUtils.toBytes(configuration);
    }
}
