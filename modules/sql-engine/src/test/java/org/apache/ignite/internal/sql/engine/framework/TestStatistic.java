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

import java.util.Collections;
import java.util.List;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.util.ImmutableBitSet;

/**
 * A test statistic that implements all the necessary for the optimizer methods to be used to prepare a query.
 */
public class TestStatistic implements Statistic {
    private final double rowCnt;

    /**
     * Constructor.
     *
     * @param rowCnt Table row count.
     */
    public TestStatistic(double rowCnt) {
        this.rowCnt = rowCnt;
    }

    /** {@inheritDoc} */
    @Override
    public Double getRowCount() {
        return rowCnt;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isKey(ImmutableBitSet cols) {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public List<ImmutableBitSet> getKeys() {
        throw new AssertionError();
    }

    /** {@inheritDoc} */
    @Override
    public List<RelReferentialConstraint> getReferentialConstraints() {
        throw new AssertionError();
    }

    /** {@inheritDoc} */
    @Override
    public List<RelCollation> getCollations() {
        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override
    public RelDistribution getDistribution() {
        throw new AssertionError();
    }
}
