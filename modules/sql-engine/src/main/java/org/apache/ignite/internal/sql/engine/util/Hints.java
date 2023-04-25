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

package org.apache.ignite.internal.sql.engine.util;

import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import org.apache.calcite.rel.hint.RelHint;

/**
 * Enumeration of supported SQL hints together with help methods.
 */
public enum Hints {
    ENFORCE_JOIN_ORDER,
    DISABLE_RULE(true),
    EXPAND_DISTINCT_AGG;

    private boolean paramSupport;

    /**
     * Constructor.
     *
     * @param paramSupport Flag indicating whether hint options are supported.
     */
    Hints(boolean paramSupport) {
        this.paramSupport = paramSupport;
    }

    /**
     * Constructor. Use for hint without parameters support.
     */
    Hints() {
        this(false);
    }

    /**
     * Check is passed hint belong to concrete hint type.
     *
     * @param hint Checking hint.
     * @return {@code true} If the given hint belong to concrete type, {@code false} otherwise.
     */
    public boolean is(RelHint hint) {
        return name().equals(hint.hintName);
    }

    /**
     * Return {@code true} if the hint presents in provided hint list.
     *
     * @param hints Hints.
     * @return {@code true} If found the hint, {@code false} otherwise.
     */
    public boolean present(List<RelHint> hints) {
        return hints.stream().anyMatch(this::is);
    }

    /**
     * Return {@code true} if the the hint presents in provided hint list and options are presents.
     *
     * @param hints Hints.
     * @return {@code true} If found the hint with rules for disabling, {@code false} otherwise.
     */
    public boolean presentWithParams(List<RelHint> hints) {
        assert paramSupport;

        return hints.stream()
                .anyMatch(h -> this.is(h) && !h.listOptions.isEmpty());
    }

    /**
     * Extract parameters for the hint from given list of hints.
     *
     * @param hints Hints.
     * @return Set of parameters for the hint in case it contains in provided list of hints. For empty list of provided hints return empty
     *         set.
     */
    public Set<String> params(List<RelHint> hints) {
        assert paramSupport;

        if (nullOrEmpty(hints)) {
            return Collections.emptySet();
        }

        return hints.stream()
                .filter(this::is)
                .flatMap(h -> h.listOptions.stream())
                .collect(Collectors.toSet());
    }

    /**
     * Generate string representation of the hint. Can be used as is in query.
     *
     * @return String representation of a hint.
     */
    public String toHint() {
        return "/*+ " + name() + " */";
    }

    /**
     * Generate string representation of the hint together with a list of parameters. Can be used as is in query.
     *
     * @return String representation of a hint together with a list of parameters..
     */
    public String toHint(String... params) {
        assert paramSupport;
        StringJoiner joiner = new StringJoiner(",", "/*+ " + name() + "(", ") */");
        Arrays.stream(params).forEach(p -> joiner.add("'" + p + "'"));

        return joiner.toString();
    }
}
