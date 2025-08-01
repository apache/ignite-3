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

import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.TestOnly;

/**
 * Operation context for update operations in {@link KeyValueStorage}. Includes operation timestamp and necessary metadata in terms of an
 * {@code long index} and a {@code long term}.
 */
public class KeyValueUpdateContext {
    public final long index;

    public final long term;

    @IgniteToStringInclude
    public final HybridTimestamp timestamp;

    /**
     * Constructor.
     *
     * @param index Update command index.
     * @param term Update command term.
     * @param timestamp Update command timestamp.
     */
    public KeyValueUpdateContext(long index, long term, HybridTimestamp timestamp) {
        assert timestamp != null;

        this.index = index;
        this.term = term;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return S.toString(this);
    }

    /**
     * Returns a context instance with {@code 0} index and term values.
     */
    @TestOnly
    public static KeyValueUpdateContext kvContext(HybridTimestamp timestamp) {
        return new KeyValueUpdateContext(0, 0, timestamp);
    }
}
