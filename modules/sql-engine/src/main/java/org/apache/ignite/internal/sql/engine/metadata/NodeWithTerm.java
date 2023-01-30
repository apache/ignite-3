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

package org.apache.ignite.internal.sql.engine.metadata;

import java.io.Serializable;
import org.jetbrains.annotations.Nullable;

/**
 * Tuple representing raft group leader node consistent name with current term.
 */
public class NodeWithTerm implements Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Raft group leader node name. */
    private final String name;

    /** Raft group leader term. */
    private final long term;

    /**
     * Gets raft group leader node name.
     *
     * @return Raft group leader node name.
     */
    public String name() {
        return name;
    }

    /**
     * Gets cached raft group leader term.
     *
     * @return Cached raft group leader term.
     */
    public long term() {
        return term;
    }

    /**
     * Constructor.
     *
     * @param name Raft group leader node name.
     * @param term Raft group leader term.
     */
    public NodeWithTerm(@Nullable String name, Long term) {
        this.name = name;
        this.term = term;
    }
}
