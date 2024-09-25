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

package org.apache.ignite.internal.sql.engine.statistic;

import java.util.Iterator;
import java.util.List;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.util.Lazy;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Represent of Sql statistics with immutable state.
 */
public class StatisticSnapshot implements Statistic {
    private final Lazy<Double> rowCount;
    private final RelDistribution distribution;
    private final List<RelCollation> collations;
    private final List<RelReferentialConstraint> referentialConstraints;
    private final List<ImmutableBitSet> keys;

    /**
     * Constructor.
     *
     * @param statistic Base statistics to make snapshot.
     */
    public StatisticSnapshot(Statistic statistic) {
        this.rowCount = new Lazy<>(statistic::getRowCount);

        this.distribution = statistic.getDistribution();
        this.collations = statistic.getCollations();
        this.referentialConstraints = statistic.getReferentialConstraints();
        this.keys = statistic.getKeys();
    }

    @Override
    public Double getRowCount() {
        return rowCount.get();
    }

    @Override
    public boolean isKey(ImmutableBitSet columns) {
        if (keys == null) {
            return false;
        }

        Iterator keyIt = keys.iterator();

        ImmutableBitSet key;
        do {
            if (!keyIt.hasNext()) {
                return false;
            }

            key = (ImmutableBitSet) keyIt.next();
        } while (!columns.contains(key));

        return true;
    }

    @Override
    public @Nullable List<ImmutableBitSet> getKeys() {
        return keys;
    }

    @Override
    public @Nullable List<RelReferentialConstraint> getReferentialConstraints() {
        return referentialConstraints;
    }

    @Override
    public @Nullable List<RelCollation> getCollations() {
        return collations;
    }

    @Override
    public @Nullable RelDistribution getDistribution() {
        return distribution;
    }
}
