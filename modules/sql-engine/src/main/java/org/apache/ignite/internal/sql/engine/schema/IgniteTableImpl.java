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

import java.util.List;
import java.util.Map;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.schema.Statistic;
import org.apache.ignite.internal.sql.engine.rel.logical.IgniteLogicalTableScan;

/**
 * Table implementation for sql engine.
 */
public class IgniteTableImpl extends AbstractIgniteDataSource implements IgniteTable {

    private final Map<String, IgniteIndex> indexMap;

    /** Constructor. */
    public IgniteTableImpl(String name, int id, int version, TableDescriptor desc,
            Statistic statistic, Map<String, IgniteIndex> indexMap) {

        super(name, id, version, desc, statistic);
        this.indexMap = indexMap;
    }

    /** {@inheritDoc} */
    @Override
    public Map<String, IgniteIndex> indexes() {
        return indexMap;
    }

    /** {@inheritDoc} */
    @Override
    protected TableScan toRel(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable relOptTbl, List<RelHint> hints) {
        return IgniteLogicalTableScan.create(cluster, traitSet, hints, relOptTbl, null, null, null);
    }
}
