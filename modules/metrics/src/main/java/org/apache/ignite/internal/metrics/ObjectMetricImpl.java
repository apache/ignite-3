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

package org.apache.ignite.internal.metrics;

import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link ObjectMetric}.
 */
public class ObjectMetricImpl<T> extends AbstractMetric implements ObjectMetric<T> {
    /** Value. */
    private volatile T val;

    /** Type. */
    private final Class<T> type;

    /**
     * Creates a new instance of ObjectMetricImpl.
     *
     * @param name Name.
     * @param desc Description.
     * @param type Type.
     */
    public ObjectMetricImpl(String name, @Nullable String desc, Class<T> type) {
        super(name, desc);

        this.type = type;
    }

    /** {@inheritDoc} */
    @Override
    public T value() {
        return val;
    }

    /**
     * Sets value.
     *
     * @param val Value.
     */
    public void value(T val) {
        this.val = val;
    }

    @Override
    public Class<T> type() {
        return type;
    }
}
