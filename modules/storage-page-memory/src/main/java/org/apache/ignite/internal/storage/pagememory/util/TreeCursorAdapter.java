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

package org.apache.ignite.internal.storage.pagememory.util;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Function;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.util.IgniteCursor;
import org.apache.ignite.lang.IgniteInternalCheckedException;

/**
 * Wraps {@link IgniteCursor} into a {@link Iterator}.
 *
 * @param <TREE_ROWT> Type of elements in a tree cursor.
 * @param <CURSOR_ROWT> Type of elements in a resulting iterator.
 */
public class TreeCursorAdapter<TREE_ROWT, CURSOR_ROWT> implements Iterator<CURSOR_ROWT> {
    /** Cursor instance from the tree. */
    private final IgniteCursor<TREE_ROWT> cursor;

    /** Value mapper to transform the data. */
    private final Function<TREE_ROWT, CURSOR_ROWT> mapper;

    /** Cached {@link IgniteCursor#next()} value. */
    private Boolean hasNext;

    public TreeCursorAdapter(IgniteCursor<TREE_ROWT> cursor, Function<TREE_ROWT, CURSOR_ROWT> mapper) {
        this.cursor = cursor;
        this.mapper = mapper;
    }

    @Override
    public boolean hasNext() throws StorageException {
        try {
            if (hasNext == null) {
                hasNext = cursor.next();
            }

            return hasNext;
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Failed to read next element from the tree", e);
        }
    }

    @Override
    public CURSOR_ROWT next() throws NoSuchElementException, StorageException {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        try {
            TREE_ROWT treeRow = cursor.get();

            hasNext = null;

            return mapper.apply(treeRow);
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Failed to read next element from the tree", e);
        }
    }
}
