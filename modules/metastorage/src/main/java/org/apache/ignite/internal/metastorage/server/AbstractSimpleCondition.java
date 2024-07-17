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

package org.apache.ignite.internal.metastorage.server;

import org.apache.ignite.internal.metastorage.Entry;

/**
 * An abstract condition which could be applied to an entry identified by the key.
 */
public abstract class AbstractSimpleCondition implements Condition {
    /** Entry key. */
    private final byte[][] keys;

    /**
     * Constructs a condition with the given entry key.
     *
     * @param key Key identifies an entry which the condition will applied to.
     */
    public AbstractSimpleCondition(byte[] key) {
        keys = new byte[][]{ key };
    }

    /** {@inheritDoc} */
    public byte[] key() {
        return keys[0];
    }

    /** {@inheritDoc} */
    @Override
    public byte[][] keys() {
        return keys;
    }

    /** {@inheritDoc} */
    @Override
    public boolean test(Entry... entries) {
        return test(entries[0]);
    }

    /**
     * Tests the given entry on condition.
     *
     * @param entry Entry which will be tested on the condition. Can't be {@code null}.
     * @return {@code True} if the given entry satisfies to the condition, otherwise - {@code false}.
     */
    protected abstract boolean test(Entry entry);
}
