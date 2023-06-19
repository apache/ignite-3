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

package org.apache.ignite.internal.sql.engine.prepare;

import java.util.List;
import org.apache.ignite.internal.sql.api.ColumnMetadataImpl;
import org.apache.ignite.internal.sql.api.ResultSetMetadataImpl;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.ResultSetMetadata;

/**
 * Distributed dml plan.
 */
public class MultiStepDmlPlan extends AbstractMultiStepPlan {
    /** DML metadata holder. */
    private static final ResultSetMetadata DML_METADATA = new ResultSetMetadataImpl(List.of(
            new ColumnMetadataImpl("ROWCOUNT", ColumnType.INT64,
                    ColumnMetadata.UNDEFINED_PRECISION, ColumnMetadata.UNDEFINED_SCALE, false, null)));

    /**
     * Constructor.
     *
     * @param queryTemplate Query execution plan template.
     * @param physPlan Physical plan, which is used for explain.
     */
    public MultiStepDmlPlan(QueryTemplate queryTemplate, IgniteRel physPlan) {
        super(queryTemplate, DML_METADATA, physPlan);
    }

    /** {@inheritDoc} */
    @Override public SqlQueryType type() {
        return SqlQueryType.DML;
    }

    /** {@inheritDoc} */
    @Override public QueryPlan copy() {
        return new MultiStepDmlPlan(queryTemplate, physPlan);
    }
}
