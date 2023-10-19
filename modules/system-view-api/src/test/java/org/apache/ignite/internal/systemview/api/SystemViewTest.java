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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.List;
import java.util.concurrent.Flow.Publisher;
import java.util.stream.Stream;
import org.apache.ignite.internal.systemview.api.NodeSystemView.Builder;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.util.SubscriptionUtils;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for {@link SystemView}.
 */
@SuppressWarnings("ThrowableNotThrown")
public class SystemViewTest {

    /** Builds a cluster view. */
    @Test
    public void buildClusterView() {
        Publisher<Dummy> dataProvider = dataProvider();

        ClusterSystemView<Dummy> view = SystemViews.<Dummy>clusterViewBuilder()
                .name("view")
                .addColumn("c1", NativeTypes.INT32, (d) -> 0)
                .addColumn("c2", NativeTypes.INT64, (d) -> 1L)
                .dataProvider(dataProvider)
                .build();

        assertEquals("VIEW", view.name(), "name");
        assertEquals(2, view.columns().size(), "columns");

        expectColumn(view.columns().get(0), "C1", NativeTypes.INT32);
        expectColumn(view.columns().get(1), "C2", NativeTypes.INT64);

        assertSame(dataProvider, view.dataProvider(), "data provider");
    }

    /** Builds a node view. */
    @Test
    public void buildNodeView() {
        Publisher<Dummy> dataProvider = dataProvider();

        NodeSystemView<Dummy> view = SystemViews.<Dummy>nodeViewBuilder()
                .name("view")
                .addColumn("c1", NativeTypes.INT32, (d) -> 0)
                .addColumn("c2", NativeTypes.INT64, (d) -> 1L)
                .nodeNameColumnAlias("node_name")
                .dataProvider(dataProvider)
                .build();

        assertEquals("VIEW", view.name(), "name");
        assertEquals(2, view.columns().size(), "columns");

        expectColumn(view.columns().get(0), "C1", NativeTypes.INT32);
        expectColumn(view.columns().get(1), "C2", NativeTypes.INT64);

        assertSame(dataProvider, view.dataProvider(), "data provider");
        assertEquals("NODE_NAME", view.nodeNameColumnAlias(), "node name column alias");
    }

    /** Reject a node view without node name alias. */
    @Test
    public void rejectNodeViewWithoutNodeNameColumnAlias() {
        assertThrowsWithCause(
                () -> {
                    SystemViews.<Dummy>nodeViewBuilder()
                            .name("name")
                            .addColumn("c1", NativeTypes.INT32, (d) -> 0)
                            .dataProvider(dataProvider())
                            .build();
                },
                IllegalArgumentException.class,
                "Node name column alias can not be null or blank"
        );
    }

    @ParameterizedTest
    @MethodSource("nullOrBlankNames")
    public void rejectNodeViewWithBlankNodeNameColumnAlias(String name) {
        assertThrowsWithCause(
                () -> {
                    SystemViews.<Dummy>nodeViewBuilder()
                            .name("name")
                            .nodeNameColumnAlias(name)
                            .addColumn("c1", NativeTypes.INT32, (d) -> 0)
                            .dataProvider(dataProvider())
                            .build();
                },
                IllegalArgumentException.class,
                "Identifier must not be null or blank"
        );
    }

    @ParameterizedTest
    @MethodSource("illegalCharsNames")
    public void rejectNodeViewWithInvalidNodeNameColumnAlias(String name) {
        assertThrowsWithCause(
                () -> {
                    SystemViews.<Dummy>nodeViewBuilder()
                            .name("name")
                            .nodeNameColumnAlias(name)
                            .addColumn("c1", NativeTypes.INT32, (d) -> 0)
                            .dataProvider(dataProvider())
                            .build();
                },
                IllegalArgumentException.class,
                "Identifier must be alphanumeric with underscore and start with letter"
        );
    }

    @Test
    public void rejectNodeViewIfColumnDuplicatesNodeNameAlias() {
        assertThrowsWithCause(
                () -> {
                    SystemViews.<Dummy>nodeViewBuilder()
                            .name("dummy")
                            .nodeNameColumnAlias("c1")
                            .addColumn("c1", NativeTypes.INT32, (d) -> 0)
                            .addColumn("c2", NativeTypes.INT64, (d) -> 1L)
                            .dataProvider(dataProvider())
                            .build();
                },
                IllegalArgumentException.class,
                "Node name column alias must distinct from column names"
        );
    }

    @Test
    public void rejectNodeViewIfNodeNameAliasDuplicatesColumn() {
        assertThrowsWithCause(
                () -> {
                    SystemViews.<Dummy>nodeViewBuilder()
                            .name("dummy")
                            .addColumn("c1", NativeTypes.INT32, (d) -> 0)
                            .addColumn("c2", NativeTypes.INT64, (d) -> 1L)
                            .nodeNameColumnAlias("c1")
                            .dataProvider(dataProvider())
                            .build();
                },
                IllegalArgumentException.class,
                "Node name column alias must distinct from column names"
        );
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
    @SuppressWarnings({"ThrowableNotThrown", "DataFlowIssue"})
    public abstract static class BuilderTest<V extends SystemView<Dummy>,
            B extends SystemView.SystemViewBuilder<V, Dummy, B>> {

        protected abstract B newBuilder();

        /** Reject a view with {@code null} name. */
        @ParameterizedTest
        @MethodSource("org.apache.ignite.internal.systemview.api.SystemViewTest#nullOrBlankNames")
        public void rejectViewWithBlankName(String name) {
            assertThrowsWithCause(() -> {
                        newBuilder()
                                .name(name)
                                .addColumn("c1", NativeTypes.INT32, (d) -> 0)
                                .dataProvider(dataProvider())
                                .build();
                    },
                    IllegalArgumentException.class,
                    "Identifier must not be null or blank"
            );
        }

        @ParameterizedTest
        @MethodSource("org.apache.ignite.internal.systemview.api.SystemViewTest#illegalCharsNames")
        public void rejectViewWithInvalidName(String name) {
            assertThrowsWithCause(
                    () -> {
                        newBuilder()
                                .name(name)
                                .addColumn("c1", NativeTypes.INT32, (d) -> 0)
                                .dataProvider(dataProvider())
                                .build();
                    },
                    IllegalArgumentException.class,
                    "Identifier must be alphanumeric with underscore and start with letter"
            );
        }

        /** Reject a view without name. */
        @Test
        public void rejectViewWithUnspecifiedName() {
            assertThrowsWithCause(
                    () -> {
                        newBuilder()
                                .addColumn("c1", NativeTypes.INT32, (d) -> 0)
                                .dataProvider(dataProvider())
                                .build();
                    },
                    IllegalArgumentException.class,
                    "Name can not be null or blank"
            );
        }

        /** Reject a view without columns. */
        @Test
        public void rejectViewWithoutColumns() {
            assertThrowsWithCause(() -> {
                        newBuilder()
                                .name("dummy")
                                .dataProvider(dataProvider())
                                .build();
                    },
                    IllegalArgumentException.class,
                    "Columns can not be empty"
            );
        }

        /** Reject a view with duplicate column names. */
        @Test
        public void rejectViewWithDuplicateColumns() {
            assertThrowsWithCause(
                    () -> {
                        newBuilder()
                                .name("dummy")
                                .addColumn("c1", NativeTypes.INT32, (d) -> 0)
                                .addColumn("c2", NativeTypes.INT64, (d) -> 1L)
                                .addColumn("c1", NativeTypes.stringOf(16), (d) -> "3")
                                .dataProvider(dataProvider())
                                .build();
                    },
                    IllegalArgumentException.class,
                    "Column names must be unique. Duplicates: [C1]"
            );
        }

        /** Reject a view with {@code null} column name. */
        @ParameterizedTest
        @MethodSource("org.apache.ignite.internal.systemview.api.SystemViewTest#nullOrBlankNames")
        public void rejectViewWithBlankColumnName(String name) {
            assertThrowsWithCause(() -> {
                        newBuilder()
                                .name("dummy")
                                .addColumn(name, NativeTypes.INT32, (d) -> 0)
                                .dataProvider(dataProvider())
                                .build();
                    },
                    IllegalArgumentException.class,
                    "Identifier must not be null or blank"
            );
        }

        /** Reject a view with invalid column name. */
        @ParameterizedTest
        @MethodSource("org.apache.ignite.internal.systemview.api.SystemViewTest#illegalCharsNames")
        public void rejectViewWithIllegalColumnName(String name) {
            assertThrowsWithCause(
                    () -> {
                        newBuilder()
                                .name("dummy")
                                .addColumn(name, NativeTypes.INT32, (d) -> 0)
                                .dataProvider(dataProvider())
                                .build();
                    },
                    IllegalArgumentException.class,
                    "Identifier must be alphanumeric with underscore and start with letter"
            );
        }

        /** Reject a view with {@code null} column type. */
        @Test
        public void rejectViewWithNullColumnType() {
            assertThrowsWithCause(() -> {
                        newBuilder()
                                .name("dummy")
                                .addColumn("c1", null, (d) -> 0)
                                .dataProvider(dataProvider())
                                .build();
                    },
                    IllegalArgumentException.class,
                    "Column type can not be null"
            );
        }

        /** Reject a view with {@code null} column value function. */
        @Test
        public void rejectViewWithNullColumnFunction() {
            assertThrowsWithCause(
                    () -> {
                        newBuilder()
                                .name("dummy")
                                .addColumn("c1", NativeTypes.INT32, null)
                                .dataProvider(dataProvider())
                                .build();
                    },
                    IllegalArgumentException.class,
                    "Column value can not be null"
            );
        }

        /** Reject a view without data provider. */
        @Test
        public void rejectViewWithoutDataProvider() {
            assertThrowsWithCause(() -> {
                        newBuilder()
                                .name("dummy")
                                .addColumn("c1", NativeTypes.INT32, (d) -> 0)
                                .build();
                    },
                    IllegalArgumentException.class,
                    "DataProvider can not be null"
            );
        }

        /** Reject a view with {@code null} data provider. */
        @Test
        public void rejectViewWithNullDataProvider() {
            assertThrowsWithCause(
                    () -> {
                        newBuilder()
                                .name("dummy")
                                .addColumn("c1", NativeTypes.INT32, (d) -> 0)
                                .dataProvider(null)
                                .build();
                    },
                    IllegalArgumentException.class,
                    "DataProvider can not be null"
            );
        }
    }

    private static void expectColumn(SystemViewColumn<?, ?> col, String name, NativeType type) {
        assertEquals(name, col.name(), "name");
        assertSame(type, col.type(), "type");
        assertNotNull(col.value(), "value");
    }

    /** Dummy system view record. */
    public static final class Dummy {

    }

    private static Publisher<Dummy> dataProvider() {
        return SubscriptionUtils.fromIterable(List.of());
    }

    private static Stream<String> nullOrBlankNames() {
        return Stream.of(null, "", " ", "  ");
    }

    private static Stream<String> illegalCharsNames() {
        return Stream.of("ASASD!@#", "_ASD", "1C", "ASD,ASD");
    }
}
