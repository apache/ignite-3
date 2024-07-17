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

package org.apache.ignite.internal.storage.index;

import static org.apache.ignite.internal.worker.ThreadAssertions.assertThreadAllowsToRead;

import org.apache.ignite.internal.worker.ThreadAssertingCursor;
import org.apache.ignite.internal.worker.ThreadAssertions;
import org.jetbrains.annotations.Nullable;

/**
 * {@link PeekCursor} that performs thread assertions when doing read operations.
 *
 * @see ThreadAssertions
 */
public class ThreadAssertingPeekCursor<T> extends ThreadAssertingCursor<T> implements PeekCursor<T> {
    private final PeekCursor<T> cursor;

    /** Constructor. */
    public ThreadAssertingPeekCursor(PeekCursor<T> cursor) {
        super(cursor);

        this.cursor = cursor;
    }

    @Override
    public @Nullable T peek() {
        assertThreadAllowsToRead();

        return cursor.peek();
    }
}
