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

package org.apache.ignite.internal.raft.storage.segstore;

import java.nio.ByteBuffer;
import org.jetbrains.annotations.Nullable;

/**
 * Class representing the result of an entry search in the log storage.
 *
 * <p>It is used to represent three conditions:
 *
 * <ol>
 *     <li>If the corresponding method returns {@code null}, then the entry was not found and we may want to continue
 *     looking in other places;</li>
 *     <li>If the method returns an {@link #isEmpty} instance, then the entry was not found but we know for sure that it does
 *     not exist in the storage;</li>
 *     <li>If the method returns a non-empty instance, then the corresponding entry has been found successfully.</li>
 * </ol>
 */
class EntrySearchResult {
    private static final EntrySearchResult EMPTY = new EntrySearchResult();

    @Nullable
    private final ByteBuffer entryBuffer;

    @SuppressWarnings("NullableProblems") // We don't want to accept nulls here.
    EntrySearchResult(ByteBuffer entryBuffer) {
        assert entryBuffer != null;

        this.entryBuffer = entryBuffer;
    }

    private EntrySearchResult() {
        entryBuffer = null;
    }

    ByteBuffer entryBuffer() {
        assert entryBuffer != null : "Search result is empty";

        return entryBuffer;
    }

    boolean isEmpty() {
        return entryBuffer == null;
    }

    static EntrySearchResult empty() {
        return EMPTY;
    }
}
