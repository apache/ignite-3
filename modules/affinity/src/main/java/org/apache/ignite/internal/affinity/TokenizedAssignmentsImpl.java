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

package org.apache.ignite.internal.affinity;

import static java.util.Collections.unmodifiableSet;

import java.util.Set;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;

/**
 * Set of nodes along with associated token that is guaranteed to be changed if the set was changed.
 */
public class TokenizedAssignmentsImpl implements TokenizedAssignments {
    private static final long serialVersionUID = -6960630542063056327L;

    @IgniteToStringInclude
    private final Set<Assignment> nodes;

    @IgniteToStringInclude
    private final long token;

    /**
     * The constructor.
     *
     * @param nodes Set of nodes.
     * @param token Token.
     */
    public TokenizedAssignmentsImpl(Set<Assignment> nodes, long token) {
        this.nodes = nodes;
        this.token = token;
    }

    @Override
    public Set<Assignment> nodes() {
        return unmodifiableSet(nodes);
    }

    @Override
    public long token() {
        return token;
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
