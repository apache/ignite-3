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

package org.apache.ignite.internal.sql.engine.schema;

import java.util.function.DoubleSupplier;
import org.apache.calcite.schema.Statistic;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.jetbrains.annotations.Nullable;

/**
 * Supported table statistics.
 */
public class IgniteStatistic implements Statistic {

    public static final double MIN_ROWS = 10_000.0;

    private final DoubleSupplier rowCountSupplier;

    private final IgniteDistribution distribution;

    private final DoubleSupplier minRows;

    /** Constructor. */
    public IgniteStatistic(DoubleSupplier rowCountSupplier, IgniteDistribution distribution) {
        this(rowCountSupplier, distribution, () -> MIN_ROWS);
    }

    /** Constructor. */
    public IgniteStatistic(DoubleSupplier rowCountSupplier, IgniteDistribution distribution, @Nullable DoubleSupplier minRows) {
        this.distribution = distribution;
        this.rowCountSupplier = rowCountSupplier;
        this.minRows = minRows == null ? () -> MIN_ROWS : minRows;
    }

    /** {@inheritDoc} */
    @Override
    public final Double getRowCount() {
        double localRowCnt = rowCountSupplier.getAsDouble();

        // Forbid zero result, to prevent zero cost for table and index scans.
        double minRows = this.minRows.getAsDouble();
        return Math.max(minRows, localRowCnt);
    }

    /** {@inheritDoc} */
    @Override
    public IgniteDistribution getDistribution() {
        return distribution;
    }
}
