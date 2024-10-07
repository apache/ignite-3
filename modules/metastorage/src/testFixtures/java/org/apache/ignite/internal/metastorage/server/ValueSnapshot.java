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

package org.apache.ignite.internal.metastorage.server;

import java.io.Serializable;
import org.apache.ignite.internal.hlc.HybridTimestamp;

/** {@link Value} container for creating and restoring from a snapshot. */
class ValueSnapshot implements Serializable {
    private static final long serialVersionUID = -435898107081479568L;

    private final byte[] bytes;

    private final boolean tombstone;

    private final HybridTimestamp operationTimestamp;

    ValueSnapshot(Value value) {
        bytes = value.bytes();
        tombstone = value.tombstone();
        operationTimestamp = value.operationTimestamp();
    }

    Value toValue() {
        return tombstone ? new Value(Value.TOMBSTONE, operationTimestamp) : new Value(bytes, operationTimestamp);
    }
}
