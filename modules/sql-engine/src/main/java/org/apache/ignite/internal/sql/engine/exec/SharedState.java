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

package org.apache.ignite.internal.sql.engine.exec;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMaps;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import org.jetbrains.annotations.Nullable;

/**
 * This class represents the volatile state that may be propagated from parent to its children
 * during rewind.
 */
public class SharedState {
    private final Long2ObjectMap<Object> correlations;

    public SharedState() {
        this(new Long2ObjectOpenHashMap<>());
    }

    SharedState(Long2ObjectMap<Object> correlations) {
        this.correlations = correlations;
    }

    /**
     * Gets correlated value.
     *
     * @param corrId Correlation ID.
     * @return Correlated value.
     */
    public @Nullable Object correlatedVariable(int corrId, int fieldIdx) {
        long key = packToLong(corrId, fieldIdx);

        return correlations.get(key);
    }

    /**
     * Sets correlated value.
     *
     * @param corrId Correlation ID.
     * @param fieldIdx Field index.
     * @param value Correlated value.
     */
    public void correlatedVariable(int corrId, int fieldIdx, @Nullable Object value) {
        long key = packToLong(corrId, fieldIdx);

        correlations.put(key, value);
    }

    Long2ObjectMap<Object> correlations() {
        return Long2ObjectMaps.unmodifiable(correlations);
    }

    private static long packToLong(int corrId, int fieldIdx) {
        assert corrId >= 0 && fieldIdx >= 0 : "corrId=" + corrId + " fieldIdx=" + fieldIdx;

        return ((((long) corrId) << 32 | fieldIdx));
    }
}
