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

import java.util.Iterator;
import java.util.function.Supplier;
import org.apache.ignite.internal.sql.engine.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.util.TransformingIterator;

/**
 * TableFunctionScan.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class TableFunctionScan<RowT> implements Iterable<RowT> {
    private final Supplier<Iterable<Object[]>> dataSupplier;

    private final RowFactory<RowT> rowFactory;

    /**
     * Constructor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public TableFunctionScan(
            Supplier<Iterable<Object[]>> dataSupplier,
            RowFactory<RowT> rowFactory
    ) {
        this.dataSupplier = dataSupplier;
        this.rowFactory = rowFactory;
    }

    /** {@inheritDoc} */
    @Override
    public Iterator<RowT> iterator() {
        return new TransformingIterator<>(dataSupplier.get().iterator(), rowFactory::create);
    }
}
