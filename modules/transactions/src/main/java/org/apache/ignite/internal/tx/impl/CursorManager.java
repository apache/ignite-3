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

package org.apache.ignite.internal.tx.impl;

import static java.util.Collections.unmodifiableMap;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.lang.ErrorGroups.Common.CURSOR_CLOSE_ERR;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Supplier;
import org.apache.ignite.internal.lang.IgniteUuid;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.exception.ReplicationException;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.IgniteException;

public class CursorManager {
    /**
     * Cursors map. The key of the map is internal Ignite uuid which consists of a transaction id ({@link UUID}) and a cursor id
     * ({@link Long}).
     */
    private final ConcurrentNavigableMap<IgniteUuid, CursorInfo> cursors = new ConcurrentSkipListMap<>(IgniteUuid.globalOrderComparator());;

    public Cursor<?> getOrCreateCursor(IgniteUuid cursorId, String txCoordinatorId, Supplier<Cursor<?>> cursorSupplier) {
        return cursors.computeIfAbsent(cursorId, k -> new CursorInfo(cursorSupplier.get(), txCoordinatorId)).cursor;
    }

    public void closeCursor(IgniteUuid cursorId) {
        CursorInfo cursorInfo = cursors.remove(cursorId);

        if (cursorInfo != null) {
            try {
                cursorInfo.cursor.close();
            } catch (Exception e) {
                throw new IgniteException(CURSOR_CLOSE_ERR, format("Close cursor exception.", e));
            }
        }
    }

    public void closeTransactionCursors(UUID txId) {
        var lowCursorId = new IgniteUuid(txId, Long.MIN_VALUE);
        var upperCursorId = new IgniteUuid(txId, Long.MAX_VALUE);

        Map<IgniteUuid, CursorInfo> txCursors = cursors(lowCursorId, upperCursorId);

        IgniteException ex = null;

        for (CursorInfo cursorInfo : txCursors.values()) {
            try {
                cursorInfo.cursor.close();
            } catch (Exception e) {
                if (ex == null) {
                    ex = new IgniteException(CURSOR_CLOSE_ERR, format("Close cursor exception.", e));
                } else {
                    ex.addSuppressed(e);
                }
            }
        }

        if (ex != null) {
            throw ex;
        }
    }

    private Map<IgniteUuid, CursorInfo> cursors(IgniteUuid lowCursorId, IgniteUuid highCursorId) {
        return cursors.subMap(lowCursorId, true, highCursorId, true);
    }

    public Map<IgniteUuid, CursorInfo> cursors() {
        return unmodifiableMap(cursors);
    }

    public static class CursorInfo {
        final Cursor<?> cursor;

        final String txCoordinatorId;

        public CursorInfo(Cursor<?> cursor, String txCoordinatorId) {
            this.cursor = cursor;
            this.txCoordinatorId = txCoordinatorId;
        }

        public Cursor<?> cursor() {
            return cursor;
        }

        public String txCoordinatorId() {
            return txCoordinatorId;
        }
    }
}
