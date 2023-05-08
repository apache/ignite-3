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
import java.util.List;
import java.util.Objects;

/**
 * Represent of hint with his options.
 */
public class Hint {
    private final IgniteHint type;

    private final List<String> options;

    /**
     * Constructor.
     *
     * @param type Ignite hint.
     */
    Hint(IgniteHint type) {
        this(type, Collections.emptyList());
    }

    /**
     * Constructor.
     *
     * @param type Ignite hint.
     * @param options List of options of the provided hint.
     */
    Hint(IgniteHint type, List<String> options) {
        assert Objects.nonNull(type);
        assert Objects.nonNull(options);

        this.type = type;
        this.options = options;
    }

    /**
     * Provide list of options for the hint.
     *
     * @return List of options for the hint.
     */
    public List<String> getOptions() {
        return options;
    }

    /**
     * Returns type of the hint.
     *
     * @return Type of the hint.
     */
    public IgniteHint getType() {
        return type;
    }
}
