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

package org.apache.ignite.internal.table.distributed.replicator;

import java.util.UUID;
import org.apache.ignite.internal.tx.impl.FullyQualifiedResourceId;

/**
 * Collection of utils to generate resource ids.
 */
public class RemoteResourceIds {

    /**
     * Generate {@link FullyQualifiedResourceId} for a transaction cursor.
     *
     * @param txId Transaction id.
     * @param cursorId Cursor id.
     * @return Cursor id.
     */
    public static FullyQualifiedResourceId cursorId(UUID txId, long cursorId) {
        return new FullyQualifiedResourceId(txId, new UUID(0L, cursorId));
    }
}
