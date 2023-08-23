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

package org.apache.ignite.internal.sql.engine.trait;

import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;

/**
 * Destination, which uses a column raw value as a target.
 */
public class Identity<RowT> implements Destination<RowT> {
    private final RowHandler<RowT> rowHandler;
    private final Set<String> nodes;
    private final int columnIndex;

    /**
     * Constructor.
     *
     * @param rowHandler Row handler.
     * @param columnIndex Column index in a row, which used for affinity calculation.
     * @param nodes Names of nodes, which are valid destinations.
     */
    public Identity(RowHandler<RowT> rowHandler, int columnIndex, List<String> nodes) {
        this.rowHandler = rowHandler;
        this.columnIndex = columnIndex;
        this.nodes = Set.copyOf(nodes);
    }

    /** {@inheritDoc} */
    @Override
    public List<String> targets(RowT row) {
        Object node = rowHandler.get(columnIndex, row);

        if (node instanceof String && nodes.contains(node)) {
            return List.of((String) node);
        }

        throw new IllegalStateException("No target found.");
    }

    /** {@inheritDoc} */
    @Override
    public Set<String> targets() {
        return nodes;
    }
}
