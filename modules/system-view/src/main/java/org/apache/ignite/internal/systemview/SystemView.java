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

import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.ignite.internal.catalog.descriptors.CatalogSystemViewDescriptor.SystemViewType;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.util.AsyncCursor;
import org.apache.ignite.internal.util.StringUtils;

/**
 * Base class for system view definitions.
 *
 * <p>Supported column types:
 * <ul>
 *     <li>boolean / {@link Boolean}</li>
 *     <li>byte / {@link Byte}</li>
 *     <li>short / {@link Short}</li>
 *     <li>int / {@link Integer}</li>
 *     <li>long / {@link Long}</li>
 *     <li>float / {@link Float}</li>
 *     <li>double / {@link Double}</li>
 *     <li>{@link String}</li>
 *     <li>array of bytes {@code byte[]}</li>
 *     <li>{@link java.math.BigDecimal}</li>
 *     <li>{@link java.time.LocalDateTime}</li>
 *     <li>{@link java.time.LocalDate}</li>
 *     <li>{@link java.time.Instant}</li>
 *     <li>{@link java.util.UUID}</li>
 *     <li>TODO Add BitSet support https://issues.apache.org/jira/browse/IGNITE-18431</li>
 *     <li>TODO Add Interval type (java.time.Period, java.time.Duration) support https://issues.apache.org/jira/browse/IGNITE-15200</li>
 * </ul>
 *
 * @param <T> System view data type.
 */
public abstract class SystemView<T> {

    private final String name;

    private final List<SystemViewColumn<T, ?>> columns;

    private final Supplier<AsyncCursor<T>> dataProvider;

    /**
     * Constructor.
     *
     * @param name View name.
     * @param columns List of columns.
     * @param dataProvider Data provider.
     */
    SystemView(String name,
            List<SystemViewColumn<T, ?>> columns,
            Supplier<AsyncCursor<T>> dataProvider) {

        if (StringUtils.nullOrBlank(name)) {
            throw new IllegalArgumentException("Name can not be null or blank");
        }

        if (columns.isEmpty()) {
            throw new IllegalArgumentException("Columns can not be empty");
        }

        List<String> duplicates = columns.stream().map(SystemViewColumn::name)
                .filter(Predicate.not(new HashSet<>()::add))
                .collect(toList());

        if (!duplicates.isEmpty()) {
            throw new IllegalArgumentException("Column names must be unique. Duplicates: " + duplicates);
        }

        if (dataProvider == null) {
            throw new IllegalArgumentException("DataProvider can not be null");
        }

        this.name = name;
        this.columns = List.copyOf(columns);
        this.dataProvider = dataProvider;
    }

    /**
     * Returns the name of this system view.
     *
     * @return The name of this system view.
     */
    public String name() {
        return name;
    }

    /**
     * Returns a list of columns of this system view.
     *
     * @return A list of view columns.
     */
    public List<SystemViewColumn<T, ?>> columns() {
        return columns;
    }

    /**
     * The data provider that produces data for this system view.
     *
     * @return The data provider.
     */
    public Supplier<AsyncCursor<T>> dataProvider() {
        return dataProvider;
    }

    /**
     * Returns the {@link SystemViewType type} of the system view.
     */
    public abstract SystemViewType type();

    /**
     * System view builder.
     *
     * @param <ViewT> System view type.
     * @param <T> System view data type.
     * @param <BuilderT> System view builder type.
     */
    public abstract static class SystemViewBuilder<ViewT extends SystemView<T>, T, BuilderT> {

        protected final List<SystemViewColumn<T, ?>> columns = new ArrayList<>();

        protected String name;

        protected Supplier<AsyncCursor<T>> dataProvider;

        /** Constructor. */
        SystemViewBuilder() {

        }

        /**
         * Sets view name.
         *
         * @param name View name.
         * @return this.
         */
        public BuilderT name(String name) {
            this.name = name;
            return (BuilderT) this;
        }

        /**
         * Adds a column.
         *
         * @param name Column name.
         * @param type Type of a column value.
         * @param value Function that extracts value of this column from a system view data record.
         * @param <C> Type of a column value.
         * @return this.
         */
        public <C> BuilderT addColumn(String name, NativeType type, Function<T, C> value) {
            columns.add(new SystemViewColumn<>(name, type, value));
            return (BuilderT) this;
        }

        /**
         * Specifies a function that produces data for this view.
         *
         * @param dataProvider Function that produces data for this view.
         * @return this.
         */
        public BuilderT dataProvider(Supplier<AsyncCursor<T>> dataProvider) {
            this.dataProvider = dataProvider;
            return (BuilderT) this;
        }

        /**
         * Creates an instance of a system view.
         *
         * @return An instance of a system view.
         */
        public abstract ViewT build();
    }
}

