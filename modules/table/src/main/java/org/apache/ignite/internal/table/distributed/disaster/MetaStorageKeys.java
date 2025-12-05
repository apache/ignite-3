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

package org.apache.ignite.internal.table.distributed.disaster;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.internal.util.ByteUtils.uuidToBytes;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.UUID;
import org.apache.ignite.internal.lang.ByteArray;

public class MetaStorageKeys {
    /** Single key for writing disaster recovery requests into meta-storage. */
    static final ByteArray RECOVERY_TRIGGER_KEY = new ByteArray("disaster.recovery.trigger");

    /**
     * Metastorage key prefix to store the per zone revision of logical event, which start the recovery process.
     * It's needed to skip the stale recovery triggers.
     */
    private static final String RECOVERY_TRIGGER_REVISION_KEY_PREFIX = "disaster.recovery.trigger.revision.";

    /** Prefix for local operations statuses. */
    static final byte[] LOCAL_OPERATIONS_PREFIX = "disaster.recovery.local.".getBytes(UTF_8);

    static final byte[] IN_PROGRESS_BYTES = "IN_PROGRESS".getBytes(UTF_8);

    static final byte[] COMPLETED_BYTES = "COMPLETED".getBytes(UTF_8);

    private static final ByteOrder BYTE_UTILS_BYTE_ORDER = ByteOrder.BIG_ENDIAN;

    static ByteArray zoneRecoveryTriggerRevisionKey(int zoneId) {
        return new ByteArray(RECOVERY_TRIGGER_REVISION_KEY_PREFIX + zoneId);
    }

    /** disaster.recovery.requests.{operationId}.{nodeName} */
    static ByteArray ongoingOperationsKey(UUID operationId, String nodeName) {
        byte[] array = ByteBuffer.allocate(LOCAL_OPERATIONS_PREFIX.length + uuidToBytes(operationId).length + 1 + nodeName.length())
                .order(BYTE_UTILS_BYTE_ORDER)
                .put(LOCAL_OPERATIONS_PREFIX)
                .put(uuidToBytes(operationId))
                .put((byte) '.')
                .put(nodeName.getBytes(UTF_8))
                .array();

        return new ByteArray(array);
    }

    /** disaster.recovery.requests.{operationId}. */
    static ByteArray operationPrefix(UUID operationId) {
        byte[] array = ByteBuffer.allocate(LOCAL_OPERATIONS_PREFIX.length + uuidToBytes(operationId).length + 1)
                .order(BYTE_UTILS_BYTE_ORDER)
                .put(LOCAL_OPERATIONS_PREFIX)
                .put(uuidToBytes(operationId))
                .put((byte) '.')
                .array();

        return new ByteArray(array);
    }
}
