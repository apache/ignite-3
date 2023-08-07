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

package org.apache.ignite.internal.sql.engine.framework;

import static org.apache.ignite.lang.IgniteStringFormatter.format;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex;
import org.jetbrains.annotations.Nullable;

/**
 * A test index that implements all the necessary for the optimizer methods to be used to prepare a query, as well as provides access to the
 * data to use this index in execution-related scenarios.
 */
public class TestIndex extends IgniteIndex {
    private static final String DATA_PROVIDER_NOT_CONFIGURED_MESSAGE_TEMPLATE =
            "DataProvider is not configured [index={}, node={}]";

    /** Factory method for creating hash-index. */
    static TestIndex createHash(String name, List<String> indexedColumns, Map<String, DataProvider<?>> dataProviders) {
        return new TestIndex(name, Type.HASH, indexedColumns, null, dataProviders);
    }

    /** Factory method for creating sorted-index. */
    static TestIndex createSorted(String name, List<String> columns, List<Collation> collations,
            Map<String, DataProvider<?>> dataProviders) {
        return new TestIndex(name, Type.SORTED, columns, collations, dataProviders);
    }

    private static final AtomicInteger ID = new AtomicInteger();

    private final int id = ID.incrementAndGet();
    private final String name;

    private final Map<String, DataProvider<?>> dataProviders;

    /** Constructor. */
    TestIndex(
            String name,
            Type type,
            List<String> columns,
            @Nullable List<Collation> collations,
            Map<String, DataProvider<?>> dataProviders
    ) {
        super(type, columns, collations);

        this.name = name;
        this.dataProviders = dataProviders;
    }

    /** Returns an id of the index. */
    @Override
    public int id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override
    public String name() {
        return name;
    }

    /**
     * Returns the data provider for the given node.
     *
     * @param nodeName Name of the node of interest.
     * @param <RowT> A type of the rows the data provider should produce.
     * @return A data provider for the node of interest.
     * @throws AssertionError in case data provider is not configured for the given node.
     */
    <RowT> DataProvider<RowT> dataProvider(String nodeName) {
        if (!dataProviders.containsKey(nodeName)) {
            throw new AssertionError(format(DATA_PROVIDER_NOT_CONFIGURED_MESSAGE_TEMPLATE, name(), nodeName));
        }

        return (DataProvider<RowT>) dataProviders.get(nodeName);
    }
}
