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

package org.apache.ignite.internal.sql.engine.util;

import java.util.Iterator;
import java.util.NoSuchElementException;
import org.jetbrains.annotations.Nullable;

/**
 * Iterator, that chains several iterator into a single one.
 *
 * @param <T> A type of the item.
 */
public class ChainedIterator<T> implements Iterator<T> {
    private final Iterator<T>[] iterators;

    private int idx;
    private @Nullable Iterator<T> current;

    /** Constructor. */
    @SafeVarargs
    public ChainedIterator(Iterator<T>... iterators) {
        this.iterators = iterators;
    }

    @Override
    public boolean hasNext() {
        if (current != null && current.hasNext()) {
            return true;
        }

        if (idx >= iterators.length) {
            return false;
        }

        do {
            current = iterators[idx];

            iterators[idx++] = null;
        } while (!current.hasNext() && idx < iterators.length);

        if (current.hasNext()) {
            return true;
        }

        current = null;

        return false;
    }

    @Override
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        assert current != null;

        return current.next();
    }
}
