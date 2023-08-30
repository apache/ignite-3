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

package org.apache.ignite.internal.systemview;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.ignite.internal.systemview.NodeSystemView.Builder;
import org.apache.ignite.internal.util.AsyncCursor;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for {@link SystemView}.
 */
public class SystemViewTest {

    /** Builds a cluster view. */
    @Test
    public void buildClusterView() {
        Supplier<AsyncCursor<Dummy>> dataProvider = dataProvider();

        ClusterSystemView<Dummy> view = SystemViews.<Dummy>clusterViewBuilder()
                .name("view")
                .addColumn("c1", int.class, (d) -> 0)
                .addColumn("c2", Long.class, (d) -> 1L)
                .dataProvider(dataProvider)
                .build();

        assertEquals("view", view.name(), "name");
        assertEquals(2, view.columns().size(), "columns");

        expectColumn(view.columns().get(0), "c1", int.class);
        expectColumn(view.columns().get(1), "c2", Long.class);

        assertSame(dataProvider, view.dataProvider(), "data provider");
    }

    /** Builds a node view. */
    @Test
    public void buildNodeView() {
        Supplier<AsyncCursor<Dummy>> dataProvider = dataProvider();

        NodeSystemView<Dummy> view = SystemViews.<Dummy>nodeViewBuilder()
                .name("view")
                .addColumn("c1", int.class, (d) -> 0)
                .addColumn("c2", Long.class, (d) -> 1L)
                .nodeNameColumnAlias("node_name")
                .dataProvider(dataProvider)
                .build();

        assertEquals("view", view.name(), "name");
        assertEquals(2, view.columns().size(), "columns");

        expectColumn(view.columns().get(0), "c1", int.class);
        expectColumn(view.columns().get(1), "c2", Long.class);

        assertSame(dataProvider, view.dataProvider(), "data provider");
        assertEquals("node_name", view.nodeNameColumnAlias(), "node name column alias");
    }

    /** Reject a node view without node name alias. */
    @Test
    public void rejectNodeViewWithoutNodeNameColumnAlias() {
        expectThrows(IllegalArgumentException.class, () -> {
            SystemViews.<Dummy>nodeViewBuilder()
                    .name("name")
                    .addColumn("c1", int.class, (d) -> 0)
                    .dataProvider(dataProvider())
                    .build();
        }, "Node name column alias can not be null or blank");
    }

    /**
     * Tests for {@link NodeSystemView.Builder}.
     */
    @Nested
    public class NodeViewBuilderTest extends BuilderTest<NodeSystemView<Dummy>, Builder<Dummy>> {

        @Override
        protected NodeSystemView.Builder<Dummy> newBuilder() {
            return SystemViews.nodeViewBuilder();
        }
    }

    /**
     * Tests for {@link ClusterSystemView.Builder}.
     */
    @Nested
    public class ClusterViewBuilderTest extends BuilderTest<ClusterSystemView<Dummy>, ClusterSystemView.Builder<Dummy>> {

        @Override
        protected ClusterSystemView.Builder<Dummy> newBuilder() {
            return SystemViews.clusterViewBuilder();
        }
    }

    /**
     * Common tests for view builder classes.
     *
     * @param <V> View type.
     * @param <B> Builder type.
     */
    public abstract static class BuilderTest<V extends SystemView<Dummy>,
            B extends SystemView.SystemViewBuilder<V, Dummy, B>> {

        protected abstract B newBuilder();

        /** Reject a view with {@code null} name. */
        @ParameterizedTest
        @MethodSource("invalidNames")
        public void rejectViewWithNullName(String name) {
            expectThrows(IllegalArgumentException.class, () -> {
                newBuilder()
                        .name(name)
                        .addColumn("c1", int.class, (d) -> 0)
                        .dataProvider(dataProvider())
                        .build();
            }, "Name can not be null or blank");
        }

        /** Reject a view without name. */
        @Test
        public void rejectViewWithUnspecifiedName() {
            expectThrows(IllegalArgumentException.class, () -> {
                newBuilder()
                        .addColumn("c1", int.class, (d) -> 0)
                        .dataProvider(dataProvider())
                        .build();
            }, "Name can not be null or blank");
        }

        /** Reject a view without columns. */
        @Test
        public void rejectViewWithoutColumns() {
            expectThrows(IllegalArgumentException.class, () -> {
                newBuilder()
                        .name("dummy")
                        .dataProvider(dataProvider())
                        .build();
            }, "Columns can not be empty");
        }

        /** Reject a view without columns. */
        @Test
        public void rejectViewWithDuplicateColumns() {
            expectThrows(IllegalArgumentException.class, () -> {
                newBuilder()
                        .name("dummy")
                        .addColumn("c1", int.class, (d) -> 0)
                        .addColumn("c2", long.class, (d) -> 1L)
                        .addColumn("c1", String.class, (d) -> "3")
                        .dataProvider(dataProvider())
                        .build();
            }, "Column names must be unique. Duplicates: [c1]");
        }

        /** Reject a view with {@code null} column name. */
        @ParameterizedTest
        @MethodSource("invalidNames")
        public void rejectViewWithNullColumnName(String name) {
            expectThrows(IllegalArgumentException.class, () -> {
                newBuilder()
                        .name("dummy")
                        .addColumn(name, int.class, (d) -> 0)
                        .dataProvider(dataProvider())
                        .build();
            }, "Column name can not be null or blank");
        }

        /** Reject a view with {@code null} column type. */
        @Test
        public void rejectViewWithNullColumnType() {
            expectThrows(NullPointerException.class, () -> {
                newBuilder()
                        .name("dummy")
                        .addColumn("c1", null, (d) -> 0)
                        .dataProvider(dataProvider())
                        .build();
            }, "Column type null can not be null");
        }

        /** Reject a view with {@code null} column value function. */
        @Test
        public void rejectViewWithNullColumnFunction() {
            expectThrows(NullPointerException.class, () -> {
                newBuilder()
                        .name("dummy")
                        .addColumn("c1", int.class, null)
                        .dataProvider(dataProvider())
                        .build();
            }, "Column value null can not be null");
        }

        /** Reject a view without data provider. */
        @Test
        public void rejectViewWithoutDataProvider() {
            expectThrows(NullPointerException.class, () -> {
                newBuilder()
                        .name("dummy")
                        .addColumn("c1", int.class, (d) -> 0)
                        .build();
            }, "DataProvider can not be null");
        }

        /** Reject a view with {@code null} data provider. */
        @Test
        public void rejectViewWithoutNullDataProvider() {
            expectThrows(NullPointerException.class, () -> {
                newBuilder()
                        .name("dummy")
                        .addColumn("c1", int.class, (d) -> 0)
                        .dataProvider(null)
                        .build();
            }, "DataProvider can not be null");
        }

        static Stream<String> invalidNames() {
            return Stream.of(null, "", " ", "  ");
        }
    }

    private static void expectColumn(SystemViewColumn<?, ?> col, String name, Class<?> type) {
        assertEquals(name, col.name(), "name");
        assertSame(type, col.type(), "type");
        assertNotNull(col.value(), "value");
    }

    /** Dummy system view record. */
    public static final class Dummy {

    }

    private static void expectThrows(Class<? extends RuntimeException> errorClass, Executable action, String errorMessage) {
        RuntimeException t = assertThrows(errorClass, action);
        assertEquals(errorMessage, t.getMessage(), "error message");
    }

    private static Supplier<AsyncCursor<Dummy>> dataProvider() {
        return () -> new AsyncCursor<>() {
            @Override
            public CompletableFuture<BatchedResult<Dummy>> requestNextAsync(int rows) {
                return CompletableFuture.completedFuture(new BatchedResult<>(List.of(), false));
            }

            @Override
            public CompletableFuture<Void> closeAsync() {
                return CompletableFuture.completedFuture(null);
            }
        };
    }
}
