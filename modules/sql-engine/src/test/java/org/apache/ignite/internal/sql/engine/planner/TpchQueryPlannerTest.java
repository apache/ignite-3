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

import static org.apache.ignite.internal.sql.engine.planner.AbstractTpcQueryPlannerTest.TpcSuiteInfo;

import org.apache.ignite.internal.sql.engine.util.tpch.TpchHelper;
import org.apache.ignite.internal.sql.engine.util.tpch.TpchTables;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests ensures a planner generates optimal plan for TPC-H queries.
 *
 * @see org.apache.ignite.internal.sql.engine.benchmarks.TpchParseBenchmark
 */
@TpcSuiteInfo(
        tables = TpchTables.class,
        queryLoader = "getQueryString",
        planLoader = "getQueryPlan"
)
public class TpchQueryPlannerTest extends AbstractTpcQueryPlannerTest {
    @ParameterizedTest
    @ValueSource(strings = {
            "1", "2", "3", "4", "5", "6", "7", "8", "8v", "9", "10", "11", "12", "12v",
            "13", "14", "14v", "15", "16", "17", "18", "19", "20", "21", "22"
    })
    public void test(String queryId) {
        validateQueryPlan(queryId);
    }

    @SuppressWarnings("unused") // used reflectively by AbstractTpcQueryPlannerTest
    static String getQueryString(String queryId) {
        return TpchHelper.getQuery(queryId);
    }

    @SuppressWarnings("unused") // used reflectively by AbstractTpcQueryPlannerTest
    static String getQueryPlan(String queryId) {
        // variant query ends with "v"
        boolean variant = queryId.endsWith("v");
        int numericId;

        if (variant) {
            String idString = queryId.substring(0, queryId.length() - 1);
            numericId = Integer.parseInt(idString);
        } else {
            numericId = Integer.parseInt(queryId);
        }

        if (variant) {
            var variantQueryFile = String.format("tpch/plan/variant_q%d.plan", numericId);
            return loadFromResource(variantQueryFile);
        } else {
            var queryFile = String.format("tpch/plan/q%s.plan", numericId);
            return loadFromResource(queryFile);
        }
    }

    @SuppressWarnings("unused") // used reflectively by AbstractTpcQueryPlannerTest
    static void updateQueryPlan(String queryId, String newPlan) {
        TpcdsQueryPlannerTest.updateQueryPlan(queryId, newPlan);
    }
}
