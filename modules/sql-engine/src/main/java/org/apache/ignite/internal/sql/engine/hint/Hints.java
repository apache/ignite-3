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

import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import org.apache.calcite.rel.hint.RelHint;

/**
 * Wrapper for set of parsed supported hints.
 */
public class Hints {
    private final EnumMap<IgniteHint, Hint> hints;

    /**
     * Constructor.
     *
     * @param hints Map of supported hints.
     */
    private Hints(EnumMap<IgniteHint, Hint> hints) {
        this.hints = hints;
    }

    /**
     * Extract parameters for the hint from given list of hints.
     *
     * @param igniteHint Hint to get parameters.
     * @return List of parameters for the hint in case it contains for provided hint, or empty list otherwise.
     */
    public List<String> params(IgniteHint igniteHint) {
        Hint hint = hints.get(igniteHint);
        if (hint == null) {
            return Collections.emptyList();
        }

        return hint.getOptions();
    }

    /**
     * Return {@code true} if the hint presents in parsed hint list.
     *
     * @param igniteHint Hints.
     * @return {@code true} If found the hint, {@code false} otherwise.
     */
    public boolean present(IgniteHint igniteHint) {
        return hints.containsKey(igniteHint);
    }

    /**
     * Parse provided list of hints and return only supported ones.
     *
     * @param relHints List of hints.
     * @return Represents supported hints which are present in the provided list of hints.
     */
    public static Hints parse(List<RelHint> relHints) {
        EnumMap<IgniteHint, Hint> supportedHints = new EnumMap<>(IgniteHint.class);

        for (RelHint h : relHints) {
            IgniteHint hints = IgniteHint.get(h.hintName);
            if (hints != null) {
                Hint hint;
                if (hints.paramSupport()) {
                    hint = new Hint(hints, h.listOptions);
                } else {
                    hint = new Hint(hints);
                }

                supportedHints.put(hints, hint);
            }
        }
        return new Hints(supportedHints);
    }
}
