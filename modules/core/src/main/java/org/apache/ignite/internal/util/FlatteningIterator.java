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

package org.apache.ignite.internal.util;

import java.util.Iterator;
import java.util.NoSuchElementException;
import org.jetbrains.annotations.Nullable;

/**
 * An iterator which implements FLAT MAP operation on the input.
 */
public class FlatteningIterator<T> implements Iterator<T>, AutoCloseable {
    private final Iterator<? extends Iterable<T>> source;

    private @Nullable Iterator<T> current;

    /** Constructs the object. */
    public FlatteningIterator(Iterator<? extends Iterable<T>> source) {
        this.source = source;
    }

    @Override
    public boolean hasNext() {
        do {
            if (current != null) {
                if (current.hasNext()) {
                    return true;
                }

                // current is completely drained, reset.
                current = null;
            }

            if (!source.hasNext()) {
                return false;
            }

            current = source.next().iterator();
        } while (true);
    }

    /** {@inheritDoc} */
    @Override
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        assert current != null;

        return current.next();
    }

    /** {@inheritDoc} */
    @Override
    public void remove() {
        if (current == null) {
            throw new IllegalStateException();
        }

        current.remove();
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws Exception {
        if (source instanceof AutoCloseable) {
            ((AutoCloseable) source).close();
        }
    }
}
