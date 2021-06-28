/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.util;

import java.util.Iterator;
import java.util.function.Function;

public class TransformingIterator<Tin, Tout> implements Iterator<Tout> {
    private final Iterator<Tin> delegate;

    private final Function<Tin, Tout> transformation;

    public TransformingIterator(Iterator<Tin> delegate, Function<Tin, Tout> transformation) {
        this.delegate = delegate;
        this.transformation = transformation;
    }

    /** {@inheritDoc} */
    @Override public boolean hasNext() {
        return delegate.hasNext();
    }

    /** {@inheritDoc} */
    @Override public Tout next() {
        return transformation.apply(delegate.next());
    }

    /** {@inheritDoc} */
    @Override public void remove() {
        delegate.remove();
    }
}
