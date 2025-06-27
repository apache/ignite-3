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

import java.util.function.LongSupplier;
import org.apache.calcite.schema.Statistic;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;

/**
 * Supported table statistics.
 */
public class IgniteStatistic implements Statistic {
    private final LongSupplier rowCountSupplier;

    private final IgniteDistribution distribution;

    /** Constructor. */
    public IgniteStatistic(LongSupplier rowCountSupplier, IgniteDistribution distribution) {
        this.distribution = distribution;
        this.rowCountSupplier = rowCountSupplier;
    }

    /** {@inheritDoc} */
    @Override
    public final Double getRowCount() {
        long approximateRowCount = rowCountSupplier.getAsLong();

        return (double) approximateRowCount;
    }

    /** {@inheritDoc} */
    @Override
    public IgniteDistribution getDistribution() {
        return distribution;
    }
}
