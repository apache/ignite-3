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

package org.apache.ignite.internal.sql.engine.exec.rel;

import java.util.BitSet;
import java.util.concurrent.Flow.Publisher;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.row.InternalTuple;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.ScannableDataSource;
import org.apache.ignite.internal.sql.engine.util.FieldDeserializingProjectedTuple;
import org.apache.ignite.internal.util.subscription.TransformingPublisher;
import org.jetbrains.annotations.Nullable;

/**
 * Execution node for scan over arbitrary {@link ScannableDataSource data source}.
 */
public class DataSourceScanNode<RowT> extends StorageScanNode<RowT> {

    private final ScannableDataSource dataSource;

    private final Function<InternalTuple, RowT> converter;

    /**
     * Constructor.
     *
     * @param ctx Execution context.
     * @param rowFactory Row factory.
     * @param schema Schema of the tuples returned by data source.
     * @param dataSource A data source to scan.
     * @param filters Optional filter to filter out rows.
     * @param rowTransformer Optional projection function.
     * @param requiredColumns Optional set of column of interest.
     */
    public DataSourceScanNode(
            ExecutionContext<RowT> ctx,
            RowHandler.RowFactory<RowT> rowFactory,
            BinaryTupleSchema schema,
            ScannableDataSource dataSource,
            @Nullable Predicate<RowT> filters,
            @Nullable Function<RowT, RowT> rowTransformer,
            @Nullable BitSet requiredColumns
    ) {
        super(ctx, filters, rowTransformer);

        this.dataSource = dataSource;

        if (requiredColumns == null || requiredColumns.cardinality() == schema.elementCount()) {
            converter = rowFactory::create;
        } else {
            int[] mapping = requiredColumns.stream().toArray();

            converter = tuple -> rowFactory.create(new FieldDeserializingProjectedTuple(schema, tuple, mapping));
        }
    }

    /** {@inheritDoc} */
    @Override
    protected Publisher<RowT> scan() {
        return new TransformingPublisher<>(dataSource.scan(), converter);
    }
}

