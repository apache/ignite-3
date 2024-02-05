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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex;
import org.apache.ignite.internal.sql.engine.schema.IgniteTableImpl;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;

/**
 * A test table that implements all the necessary for the optimizer methods to be used to prepare a query, as well as provides access to the
 * data to use this table in execution-related scenarios.
 */
public class TestTable extends IgniteTableImpl {
    private static final String DATA_PROVIDER_NOT_CONFIGURED_MESSAGE_TEMPLATE =
            "DataProvider is not configured [table={}, node={}]";

    static final AtomicInteger ID = new AtomicInteger();

    private final Map<String, DataProvider<?>> dataProviders;

    /** Constructor. */
    public TestTable(
            int tableId,
            TableDescriptor descriptor,
            String name,
            double rowCnt,
            List<IgniteIndex> indexes
    ) {
        this(tableId, descriptor, name, rowCnt, indexes, Map.of());
    }

    /** Constructor. */
    public TestTable(
            int tableId,
            TableDescriptor descriptor,
            String name,
            double rowCnt,
            List<IgniteIndex> indexList,
            Map<String, DataProvider<?>> dataProviders
    ) {
        super(name, tableId, 1, descriptor, new TestStatistic(rowCnt),
                indexList.stream().collect(Collectors.toUnmodifiableMap(IgniteIndex::name, Function.identity())), 1);

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
