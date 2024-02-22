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
import static org.apache.ignite.lang.ErrorGroups.Common.CURSOR_CLOSING_ERR;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Supplier;
import org.apache.ignite.internal.lang.IgniteUuid;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.IgniteException;

/**
 * This registry keeps track of the created cursors.
 */
public class CursorRegistry {
    /**
     * Cursors map. The key of the map is cursor id which consists of a prefix id ({@link UUID}) and a cursor local id
     * ({@link Long}).
     */
    private final ConcurrentNavigableMap<IgniteUuid, CursorInfo> cursors = new ConcurrentSkipListMap<>(IgniteUuid.globalOrderComparator());

    /**
     * Register a cursor.
     *
     * @param cursorId Cursor id.
     * @param cursorCreatorId Node id of the creator of the cursor.
     * @param cursorSupplier Supplier to create a cursor.
     * @return Cursor.
     */
    public Cursor<?> registerCursor(IgniteUuid cursorId, String cursorCreatorId, Supplier<Cursor<?>> cursorSupplier) {
        return cursors.computeIfAbsent(cursorId, k -> new CursorInfo(cursorSupplier.get(), cursorCreatorId)).cursor;
    }

    /**
     * Close the given cursor.
     *
     * @param cursorId Cursor id.
     */
    public void closeCursor(IgniteUuid cursorId) {
        CursorInfo cursorInfo = cursors.remove(cursorId);

        if (cursorInfo != null) {
            try {
                cursorInfo.cursor.close();
            } catch (Exception e) {
                throw new IgniteException(CURSOR_CLOSING_ERR, format("Close cursor exception.", e));
            }
        }
    }

    /**
     * Close all cursors created sharing the same prefix.
     *
     * @param cursorIdPrefix Common prefix of the ids of cursors.
     */
    public void closeCursorsWithPrefix(UUID cursorIdPrefix) {
        Map<IgniteUuid, CursorInfo> txCursors = cursors(cursorIdPrefix);

        IgniteException ex = null;

        for (CursorInfo cursorInfo : txCursors.values()) {
            try {
                cursorInfo.cursor.close();
            } catch (Exception e) {
                if (ex == null) {
                    ex = new IgniteException(CURSOR_CLOSING_ERR, format("Close cursor exception.", e));
                } else {
                    ex.addSuppressed(e);
                }
            }
        }

        txCursors.clear();

        if (ex != null) {
            throw ex;
        }
    }

    private Map<IgniteUuid, CursorInfo> cursors(UUID cursorIdPrefix) {
        var lowCursorId = new IgniteUuid(cursorIdPrefix, Long.MIN_VALUE);
        var upperCursorId = new IgniteUuid(cursorIdPrefix, Long.MAX_VALUE);

        return cursors.subMap(lowCursorId, true, upperCursorId, true);
    }

    /**
     * Returns all cursors.
     *
     * @return Cursors.
     */
    public Map<IgniteUuid, CursorInfo> cursors() {
        return unmodifiableMap(cursors);
    }

    /**
     * Cursor information.
     */
    public static class CursorInfo {
        private final Cursor<?> cursor;

        private final String cursorCreatorId;

        public CursorInfo(Cursor<?> cursor, String cursorCreatorId) {
            this.cursor = cursor;
            this.cursorCreatorId = cursorCreatorId;
        }

        /**
         * Cursor.
         *
         * @return Cursor.
         */
        public Cursor<?> cursor() {
            return cursor;
        }

        /**
         * Node inconsistent id of the creator of the cursor.
         *
         * @return Node id of the creator of the cursor.
         */
        public String cursorCreatorId() {
            return cursorCreatorId;
        }
    }
}
