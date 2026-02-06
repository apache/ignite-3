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

package org.apache.ignite.internal.sql.engine.hint;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Enumeration of supported SQL hints together with help methods.
 */
public enum IgniteHint {
    /** Only for test purpose!!! No any product use and not available for user. */
    DISABLE_RULE(true),
    /** Disable commute joins and keeps the join order defined by the user in a query. */
    ENFORCE_JOIN_ORDER,
    /** Force expanding distinct aggregates to join. */
    EXPAND_DISTINCT_AGG,
    /** Disables index usage. **/
    NO_INDEX(true),
    /** Forces index usage. */
    FORCE_INDEX(true),
    /** Disable optimizations related to subquery decorrelation. */
    DISABLE_DECORRELATION,
    ;

    private final boolean paramSupport;

    private static final Map<String, IgniteHint> ALL_HINTS_AS_MAP = new HashMap<>();

    static {
        Arrays.stream(values()).forEach(e -> ALL_HINTS_AS_MAP.put(e.name(), e));
    }

    /**
     * Constructor.
     *
     * @param paramSupport Flag indicating whether hint options are supported.
     */
    IgniteHint(boolean paramSupport) {
        this.paramSupport = paramSupport;
    }

    /**
     * Constructor. Use for hint without parameters support.
     */
    IgniteHint() {
        this(false);
    }

    /**
     * Returns enum instance by they name.
     *
     * @param hintName Name of hint.
     *
     * @return IgniteHint instance if matched instance found, {@code null} otherwise.
     */
    public static IgniteHint get(String hintName) {
        return ALL_HINTS_AS_MAP.get(hintName);
    }

    /**
     * Returns whether parameter supports by the hint.
     *
     * @return {@code true} If the hint has parameter support, {@code false} otherwise.
     */
    public boolean paramSupport() {
        return paramSupport;
    }
}
