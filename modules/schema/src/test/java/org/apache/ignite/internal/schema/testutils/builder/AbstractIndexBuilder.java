/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.schema.testutils.builder;

import java.util.Collections;
import java.util.Map;
import org.apache.ignite.internal.util.IgniteObjectName;

/**
 * Index base class.
 */
public abstract class AbstractIndexBuilder implements SchemaObjectBuilder {
    /** Index name. */
    protected final String name;

    /** Unique flag. */
    private boolean unique;

    /** Builder hints. */
    protected Map<String, String> hints;

    /**
     * Constructor.
     *
     * @param name Index name.
     */
    AbstractIndexBuilder(String name) {
        this(name, false);
    }

    /**
     * Constructor.
     *
     * @param name   Index name.
     * @param unique Unique flag.
     */
    AbstractIndexBuilder(String name, boolean unique) {
        this.name = IgniteObjectName.parse(name);
        this.unique = unique;
    }

    /**
     * Unique index flag.
     *
     * @return Unique flag.
     */
    public boolean unique() {
        return unique;
    }

    /**
     * Sets unique index flag.
     *
     * @return {@code This} for chaining.
     */
    public AbstractIndexBuilder unique(boolean unique) {
        this.unique = unique;

        return this;
    }

    /** {@inheritDoc} */
    @Override
    public AbstractIndexBuilder withHints(Map<String, String> hints) {
        this.hints = Collections.unmodifiableMap(hints);

        return this;
    }
}
