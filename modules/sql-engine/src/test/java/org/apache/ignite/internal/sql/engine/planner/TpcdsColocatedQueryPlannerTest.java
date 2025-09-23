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

import org.apache.ignite.internal.lang.IgniteSystemProperties;
import org.apache.ignite.internal.sql.engine.planner.AbstractTpcQueryPlannerTest.TpcSuiteInfo;
import org.apache.ignite.internal.sql.engine.util.tpcds.TpcdsTables;
import org.apache.ignite.internal.testframework.WithSystemProperty;

/**
 * Tests ensures a planner generates optimal plan for TPC-DS queries when the colocation feature is enabled.
 */
@TpcSuiteInfo(
        tables = TpcdsTables.class,
        queryLoader = "getQueryString",
        planLoader = "getQueryPlan"
)
@WithSystemProperty(key = IgniteSystemProperties.COLOCATION_FEATURE_FLAG, value = "true")
public class TpcdsColocatedQueryPlannerTest extends TpcdsQueryPlannerTest {
    @SuppressWarnings("unused") // used reflectively by AbstractTpcQueryPlannerTest
    static String getQueryString(String queryId) {
        return TpcdsQueryPlannerTest.getQueryString(queryId);
    }

    @SuppressWarnings("unused") // used reflectively by AbstractTpcQueryPlannerTest
    static String getQueryPlan(String queryId) {
        return TpcdsQueryPlannerTest.getQueryPlan(queryId);
    }

    @SuppressWarnings("unused") // used reflectively by AbstractTpcQueryPlannerTest
    static void updateQueryPlan(String queryId, String newPlan) {
        TpcdsQueryPlannerTest.updateQueryPlan(queryId, newPlan);
    }
}
