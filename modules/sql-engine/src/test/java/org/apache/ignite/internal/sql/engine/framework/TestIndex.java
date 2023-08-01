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

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.calcite.rel.RelCollation;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex.Type;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchemaIndex;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;

/**
 * A test index that implements all the necessary for the optimizer methods to be used to prepare a query, as well as provides access to the
 * data to use this index in execution-related scenarios.
 */
public class TestIndex extends IgniteSchemaIndex {
    private static final String DATA_PROVIDER_NOT_CONFIGURED_MESSAGE_TEMPLATE =
            "DataProvider is not configured [index={}, node={}]";

    /** Factory method for creating hash-index. */
    static TestIndex createHash(String name, IgniteDistribution distribution, RelCollation collation,
            Map<String, DataProvider<?>> dataProviders) {
        return new TestIndex(name, Type.HASH, distribution, collation, dataProviders);
    }

    /** Factory method for creating sorted-index. */
    static TestIndex createSorted(String name, IgniteDistribution distribution, RelCollation collation,
            Map<String, DataProvider<?>> dataProviders) {
        return new TestIndex(name, Type.SORTED, distribution, collation, dataProviders);
    }

    private static final AtomicInteger ID = new AtomicInteger();

    private final Map<String, DataProvider<?>> dataProviders;

    /** Constructor. */
    TestIndex(
            String name,
            Type type,
            IgniteDistribution distribution,
            RelCollation collation,
            Map<String, DataProvider<?>> dataProviders
    ) {
        super(ID.incrementAndGet(), name, type, distribution, collation);

        this.dataProviders = dataProviders;
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
