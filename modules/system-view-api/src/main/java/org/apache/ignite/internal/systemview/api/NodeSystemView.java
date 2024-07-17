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

package org.apache.ignite.internal.systemview.api;

import java.util.List;
import java.util.concurrent.Flow.Publisher;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.StringUtils;

/**
 * Node system view definition.
 *
 * <pre>
 *   // Creates definition of a node system view.
 *   var connectionsView = SystemViews.&lt;Connection&gt;nodeViewBuilder()
 *     .name("CONNECTIONS")
 *     .addColumn("USER", Integer.class, Connection::user)
 *     .addColumn("REMOTE_ADDRESS", String.class, Connection::remoteAddress)
 *     .addColumn("LOCAL_ADDRESS", String.class, Connection::localAddress)
 *     .dataProvider(() -> toAsyncCursor(connManager.connections()))
 *     .build();
 * </pre>
 *
 * @param <T> System view data type.
 * @see SystemView
 */
public class NodeSystemView<T> extends SystemView<T> {

    private final String nodeNameColumnAlias;

    /**
     * Constructor.
     *
     * @param name View name.
     * @param columns List of columns.
     * @param dataProvider Data provider.
     * @param nodeNameColumnAlias Node name column alias.
     */
    private NodeSystemView(String name,
            List<SystemViewColumn<T, ?>> columns,
            Publisher<T> dataProvider,
            String nodeNameColumnAlias) {
        super(name, columns, dataProvider);

        if (StringUtils.nullOrBlank(nodeNameColumnAlias)) {
            throw new IllegalArgumentException("Node name column alias can not be null or blank");
        }

        if (columns.stream().anyMatch(c -> nodeNameColumnAlias.equals(c.name()))) {
            throw new IllegalArgumentException("Node name column alias must distinct from column names");
        }

        this.nodeNameColumnAlias = nodeNameColumnAlias;
    }

    /**
     * Returns an alias of a node name column of this node system view.
     *
     * @return An alias of a node name column if set.
     */
    public String nodeNameColumnAlias() {
        return nodeNameColumnAlias;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(NodeSystemView.class, this, "name", name(), "columns", columns(), "nodeNameColumnAlias", nodeNameColumnAlias);
    }

    /**
     * Node system view builder.
     *
     * @param <T> System view data type.
     */
    public static class Builder<T> extends SystemViewBuilder<NodeSystemView<T>, T, Builder<T>> {

        private String nodeNameColumnAlias;

        /** Constructor. */
        Builder() {

        }

        /**
         * Sets an alias for a node name column. Should only be set for node system views.
         *
         * @param alias Node name column alias. Must contain only latin letters, digits and underscore.
         *      The first character must be a letter.
         * @return this.
         */
        public Builder<T> nodeNameColumnAlias(String alias) {
            this.nodeNameColumnAlias = normalizeIdentifier(alias);
            return this;
        }

        /**
         * Creates an instance of {@link NodeSystemView}.
         *
         * @return Definition of a node system view.
         */
        @Override
        public NodeSystemView<T> build() {
            return new NodeSystemView<>(name, columns, dataProvider, nodeNameColumnAlias);
        }
    }
}
