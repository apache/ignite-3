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

import static org.apache.ignite.internal.raft.storage.segstore.EntrySearchResult.SearchOutcome.CONTINUE_SEARCH;
import static org.apache.ignite.internal.raft.storage.segstore.EntrySearchResult.SearchOutcome.NOT_FOUND;
import static org.apache.ignite.internal.raft.storage.segstore.EntrySearchResult.SearchOutcome.SUCCESS;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

/**
 * Class representing the result of an entry search in the log storage.
 *
 * <p>It is used to represent three search outcomes:
 *
 * <ol>
 *     <li>If {@link EntrySearchResult#searchOutcome()} is {@link SearchOutcome#CONTINUE_SEARCH}, then the entry was not found and we should
 *     continue looking in other places;</li>
 *     <li>If {@link EntrySearchResult#searchOutcome()} is {@link SearchOutcome#NOT_FOUND}, then the entry was not found and we know for
 *     sure that it does not exist in the storage;</li>
 *     <li>If {@link EntrySearchResult#searchOutcome()} is {@link SearchOutcome#SUCCESS}, then the corresponding entry has been found
 *     successfully and {@link EntrySearchResult#entryBuffer()} method can be used to obtain the entry value.</li>
 * </ol>
 */
class EntrySearchResult {
    enum SearchOutcome {
        SUCCESS, NOT_FOUND, CONTINUE_SEARCH
    }

    private static final EntrySearchResult NOT_FOUND_RESULT = new EntrySearchResult(null, NOT_FOUND);

    private static final EntrySearchResult CONTINUE_SEARCH_RESULT = new EntrySearchResult(null, CONTINUE_SEARCH);

    @Nullable
    private final ByteBuffer entryBuffer;

    private final SearchOutcome searchOutcome;

    private EntrySearchResult(@Nullable ByteBuffer entryBuffer, SearchOutcome searchOutcome) {
        this.entryBuffer = entryBuffer;
        this.searchOutcome = searchOutcome;
    }

    ByteBuffer entryBuffer() {
        assert entryBuffer != null : "Search result is empty";

        return entryBuffer;
    }

    SearchOutcome searchOutcome() {
        return searchOutcome;
    }

    static EntrySearchResult success(ByteBuffer entryBuffer) {
        return new EntrySearchResult(entryBuffer, SUCCESS);
    }

    static EntrySearchResult notFound() {
        return NOT_FOUND_RESULT;
    }

    static EntrySearchResult continueSearch() {
        return CONTINUE_SEARCH_RESULT;
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
