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

package org.apache.ignite.internal.sql.engine.exec;

import java.util.BitSet;
import java.util.concurrent.Flow.Publisher;
import org.apache.ignite.internal.sql.engine.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.sql.engine.metadata.PartitionWithTerm;
import org.jetbrains.annotations.Nullable;

/**
 * Provides read operations on a table.
 */
public interface ScanableTable {

    /**
     * Performs a scan over table.
     *
     * @param ctx  Execution context.
     * @param partWithTerm  Partition.
     * @param rowFactory  Row factory.
     * @param requiredColumns  Required columns.
     * @return  A publisher that produces rows.
     * @param <RowT>  A type of row.
     */
    <RowT> Publisher<RowT> scan(ExecutionContext<RowT> ctx, PartitionWithTerm partWithTerm,
            RowFactory<RowT> rowFactory, @Nullable BitSet requiredColumns);

}
