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

package org.apache.ignite.internal.sql.engine.benchmarks;

import java.util.List;
import org.apache.ignite.internal.sql.engine.framework.DataProvider;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders.DataProviderFactory;
import org.apache.ignite.internal.sql.engine.schema.ColumnDescriptor;
import org.apache.ignite.internal.sql.engine.util.NativeTypeValues;
import org.apache.ignite.sql.ColumnType;

/**
 * {@link DataProviderFactory} that creates {@link DataProvider}s that generates a row of pseudo random data based on table column types
 * and then returns the same row multiple times.
 */
final class RepeatedRandomRowDataProviderFactory implements DataProviderFactory {

    private final int dataSize;

    RepeatedRandomRowDataProviderFactory(int dataSize) {
        this.dataSize = dataSize;
    }

    /** {@inheritDoc} **/
    @Override
    public DataProvider<Object[]> createDataProvider(String tableName, List<ColumnDescriptor> columns) {
        Object[] row = columns.stream().map(c -> NativeTypeValues.value(1, c.physicalType().spec())).toArray();

        return DataProvider.fromRow(row, dataSize);
    }
}
