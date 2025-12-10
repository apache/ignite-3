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

package org.apache.ignite.internal.configuration.storage;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

/**
 * Represents data in configuration storage.
 */
public class ReadEntry {
    /** Values. */
    private final Map<String, ? extends Serializable> values;

    private final Map<String, ? extends Serializable> invariant;

    /** Configuration storage version. */
    private final long changeId;

    /**
     * Constructor.
     *
     * @param values   Values.
     * @param changeId Version.
     */
    public ReadEntry(Map<String, ? extends Serializable> values, long changeId) {
        this.values = Collections.unmodifiableMap(values);
        this.changeId = changeId;
        this.invariant = Collections.emptyMap();
    }

    public ReadEntry(Map<String, ? extends Serializable> values, long changeId, Map<String, ? extends Serializable> invariant) {
        this.values = values;
        this.changeId = changeId;
        this.invariant = invariant;
    }

    /** Get values. */
    public Map<String, ? extends Serializable> values() {
        return values;
    }

    public Map<String, ? extends Serializable> invariant() {
        return invariant;
    }

    /**
     * Get version.
     *
     * @return version.
     */
    public long changeId() {
        return changeId;
    }
}
