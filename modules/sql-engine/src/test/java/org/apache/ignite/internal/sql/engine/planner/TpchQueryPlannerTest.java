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

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.io.CharStreams;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.framework.TestCluster;
import org.apache.ignite.internal.sql.engine.framework.TestNode;
import org.apache.ignite.internal.sql.engine.prepare.MultiStepPlan;
import org.apache.ignite.internal.sql.engine.util.Cloner;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.tpch.TpchHelper;
import org.apache.ignite.internal.sql.engine.util.tpch.TpchTables;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests ensures a planner generates optimal plan for TPC-H queries.
 *
 * @see org.apache.ignite.internal.sql.engine.benchmarks.TpchParseBenchmark
 */
// TODO https://issues.apache.org/jira/browse/IGNITE-21986 validate other query plans and make test parameterized.
public class TpchQueryPlannerTest extends AbstractPlannerTest {
    private static TestCluster CLUSTER;

    @BeforeAll
    static void startCluster() {
        CLUSTER = TestBuilders.cluster().nodes("N1").build();
        CLUSTER.start();

        TestNode node = CLUSTER.node("N1");

        node.initSchema(TpchTables.LINEITEM.ddlScript());
    }

    @AfterAll
    static void stopCluster() throws Exception {
        CLUSTER.stop();
        CLUSTER = null;
    }

    @Test
    public void tpchTest_q1() {
        validateQueryPlan("1");
    }

    private static void validateQueryPlan(String queryId) {
        TestNode node = CLUSTER.node("N1");

        MultiStepPlan plan = (MultiStepPlan) node.prepare(TpchHelper.getQuery(queryId));

        String actualPlan = RelOptUtil.toString(Cloner.clone(plan.root(), Commons.cluster()), SqlExplainLevel.DIGEST_ATTRIBUTES);
        String expectedPlan = getQueryPlan(queryId);

        assertEquals(expectedPlan, actualPlan);
    }

    /**
     * Loads query plan for provided TPC-H query id.
     *
     * @see TpchHelper#getQuery(String) for query id details.
     */
    public static String getQueryPlan(String queryId) {
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

    static String loadFromResource(String resource) {
        try (InputStream is = TpchHelper.class.getClassLoader().getResourceAsStream(resource)) {
            if (is == null) {
                throw new IllegalArgumentException("Resource does not exist: " + resource);
            }
            try (InputStreamReader reader = new InputStreamReader(is, StandardCharsets.UTF_8)) {
                return CharStreams.toString(reader);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("I/O operation failed: " + resource, e);
        }
    }
}
