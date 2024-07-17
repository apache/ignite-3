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
import java.util.List;
import java.util.ListIterator;

/**
 * An read-only {@link Iterator} that traverses a given list in reverse order.
 *
 * @param <T> The type of elements returned by this iterator.
 */
public class ReverseIterator<T> implements Iterator<T> {
    private final ListIterator<T> it;

    public ReverseIterator(List<T> list) {
        this.it = list.listIterator(list.size());
    }

    @Override
    public boolean hasNext() {
        return it.hasPrevious();
    }

    @Override
    public T next() {
        return it.previous();
    }
}
