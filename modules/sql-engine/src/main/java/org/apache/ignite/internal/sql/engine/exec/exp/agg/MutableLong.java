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

package org.apache.ignite.internal.sql.engine.exec.exp.agg;

/**
 * Mutable variant of {@link Long} used by some {@link Accumulator}.
 */
final class MutableLong extends Number {

    private static final long serialVersionUID = -147172356021174032L;

    private long value;

    /** Adds the given value to this long. */
    public void add(long v) {
        value += v;
    }

    /** {@inheritDoc} */
    @Override
    public int intValue() {
        return (int) value;
    }

    /** {@inheritDoc} */
    @Override
    public long longValue() {
        return value;
    }

    /** {@inheritDoc} */
    @Override
    public float floatValue() {
        return value;
    }

    /** {@inheritDoc} */
    @Override
    public double doubleValue() {
        return value;
    }
}
