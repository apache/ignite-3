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

package org.apache.ignite.internal.catalog.sql;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

class QueryPartCollection<T extends QueryPart> extends QueryPart implements Collection<T> {
    private final Collection<T> wrapped;

    private static final String SEPARATOR = ", ";

    private boolean formatSeparator = false;

    static <T extends QueryPart> QueryPartCollection<T> partsList(Collection<T> wrapped) {
        return new QueryPartCollection<>(wrapped);
    }

    @SafeVarargs
    static <T extends QueryPart> QueryPartCollection<T> partsList(T... wrapped) {
        return partsList(Arrays.asList(wrapped));
    }

    private QueryPartCollection(Collection<T> wrapped) {
        this.wrapped = wrapped;
    }

    QueryPartCollection<T> formatSeparator() {
        return formatSeparator(true);
    }

    QueryPartCollection<T> formatSeparator(boolean b) {
        this.formatSeparator = b;
        return this;
    }

    @Override
    protected void accept(QueryContext ctx) {
        boolean first = true;
        for (T part : wrapped) {
            if (!first) {
                ctx.sql(SEPARATOR);
                if (formatSeparator) {
                    ctx.formatSeparator();
                }
            }
            ctx.visit(part);
            first = false;
        }
    }

    @Override
    public int size() {
        return wrapped.size();
    }

    @Override
    public boolean isEmpty() {
        return wrapped.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return wrapped.contains(o);
    }

    @Override
    public Iterator<T> iterator() {
        return wrapped.iterator();
    }

    @Override
    public Object[] toArray() {
        return wrapped.toArray();
    }

    @Override
    public <T1> T1[] toArray(T1[] a) {
        return wrapped.toArray(a);
    }

    @Override
    public boolean add(T t) {
        return wrapped.add(t);
    }

    @Override
    public boolean remove(Object o) {
        return wrapped.remove(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return wrapped.containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        return wrapped.addAll(c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return wrapped.removeAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return wrapped.retainAll(c);
    }

    @Override
    public void clear() {
        wrapped.clear();
    }
}
