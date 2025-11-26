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

import it.unimi.dsi.fastutil.ints.IntSet;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.ignite.internal.sql.engine.util.tpcds.TpcdsHelper;
import org.apache.ignite.internal.sql.engine.util.tpcds.TpcdsTables;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junitpioneer.jupiter.params.IntRangeSource;

/**
 * Tests ensures a planner generates optimal plan for TPC-DS queries.
 */
@TpcSuiteInfo(
        tables = TpcdsTables.class,
        queryLoader = "getQueryString",
        planLoader = "getQueryPlan"
)
public class TpcdsQueryPlannerTest extends AbstractTpcQueryPlannerTest {

    private static final IntSet UNSUPPORTED_TESTS = IntSet.of(
            // TODO https://issues.apache.org/jira/browse/IGNITE-14642 Support STDDEV_SAMP function and unmute tests.
            17, 29, 35, 39,
            // TODO https://issues.apache.org/jira/browse/IGNITE-25873 Support aggregate window function RANK and unmute tests.
            44, 47, 49, 57, 67, 70, 36, 86
    );

    @ParameterizedTest
    @IntRangeSource(from = 1, to = 99, closed = true)
    public void test(int queryId) {
        Assumptions.assumeFalse(UNSUPPORTED_TESTS.contains(queryId), "unsupported query");

        validateQueryPlan(Integer.toString(queryId));
    }

    @SuppressWarnings("unused") // used reflectively by AbstractTpcQueryPlannerTest
    static String getQueryString(String queryId) {
        return TpcdsHelper.getQuery(queryId);
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
            var variantQueryFile = String.format("tpcds/plan/variant_q%d.plan", numericId);
            return loadFromResource(variantQueryFile);
        } else {
            var queryFile = String.format("tpcds/plan/q%s.plan", numericId);

            return loadFromResource(queryFile);
        }
    }

    @SuppressWarnings("unused") // used reflectively by AbstractTpcQueryPlannerTest
    static void updateQueryPlan(String queryId, String newPlan) {
        Path targetDirectory = null;

        // A targetDirectory must be specified by hand when expected plans are generated. 
        //noinspection ConstantValue 
        if (targetDirectory == null) {
            throw new RuntimeException("Please provide target directory to where save generated plans." 
                    + " Usually plans are kept in resource folder of tests within the same module.");
        }

        // variant query ends with "v"
        boolean variant = queryId.endsWith("v");
        int numericId;

        if (variant) {
            String idString = queryId.substring(0, queryId.length() - 1);
            numericId = Integer.parseInt(idString);
        } else {
            numericId = Integer.parseInt(queryId);
        }

        Path planLocation;
        if (variant) {
            planLocation = targetDirectory.resolve(String.format("variant_q%d.plan", numericId));
        } else {
            planLocation = targetDirectory.resolve(String.format("q%s.plan", numericId));
        }

        try {
            Files.writeString(planLocation, newPlan);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
