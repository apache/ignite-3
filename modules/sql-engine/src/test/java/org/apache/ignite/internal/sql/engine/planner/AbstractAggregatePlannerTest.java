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

package org.apache.ignite.internal.sql.engine.planner;

import java.util.UUID;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeSystem;

/**
 * Base class for further planner test implementations.
 */
public class AbstractAggregatePlannerTest extends AbstractPlannerTest {
    /**
     * Creates table with broadcast distribution.
     *
     * @param tblName Table name.
     * @return Table instance with broadcast distribution and multiple predefined columns.
     */
    protected TestTable createBroadcastTable(String tblName) {
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        return createTable(tblName,
                new RelDataTypeFactory.Builder(f)
                        .add("ID", f.createJavaType(Integer.class))
                        .add("VAL0", f.createJavaType(Integer.class))
                        .add("VAL1", f.createJavaType(Integer.class))
                        .add("GRP0", f.createJavaType(Integer.class))
                        .add("GRP1", f.createJavaType(Integer.class))
                        .build(),
                DEFAULT_TBL_SIZE,
                IgniteDistributions.broadcast()
        );
    }

    /**
     * Creates table with specified affinity distribution.
     *
     * @param tblName Table name.
     * @return Table instance with specified affinity distribution and multiple predefined columns.
     */
    protected AbstractPlannerTest.TestTable createAffinityTable(String tblName) {
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        return createTable(tblName,
                new RelDataTypeFactory.Builder(f)
                        .add("ID", f.createJavaType(Integer.class))
                        .add("VAL0", f.createJavaType(Integer.class))
                        .add("VAL1", f.createJavaType(Integer.class))
                        .add("GRP0", f.createJavaType(Integer.class))
                        .add("GRP1", f.createJavaType(Integer.class))
                        .build(),
                DEFAULT_TBL_SIZE,
                IgniteDistributions.affinity(0, UUID.randomUUID(), DEFAULT_ZONE_ID)
        );
    }
}
