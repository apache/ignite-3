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

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static org.apache.ignite.internal.sql.engine.util.Commons.cast;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.io.CharStreams;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.framework.TestCluster;
import org.apache.ignite.internal.sql.engine.framework.TestNode;
import org.apache.ignite.internal.sql.engine.prepare.MultiStepPlan;
import org.apache.ignite.internal.sql.engine.util.Cloner;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.TpcScaleFactor;
import org.apache.ignite.internal.sql.engine.util.TpcTable;
import org.apache.ignite.internal.sql.engine.util.tpch.TpchHelper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInfo;

/** 
 * Abstract test class to ensure a planner generates optimal plan for TPC queries.
 *
 * <p>Any derived class must be annotated with {@link TpcSuiteInfo}.
 */
abstract class AbstractTpcQueryPlannerTest extends AbstractPlannerTest {
    private static TestCluster CLUSTER;

    private static Function<String, String> queryLoader;
    private static Function<String, String> planLoader;

    @BeforeAll
    static void startCluster(TestInfo info) throws NoSuchMethodException {
        Class<?> testClass = info.getTestClass().orElseThrow();

        TpcSuiteInfo suiteInfo = testClass.getAnnotation(TpcSuiteInfo.class);

        if (suiteInfo == null) {
            throw new IllegalStateException("Class " + testClass + " must be annotated with @" + TpcSuiteInfo.class.getSimpleName());
        }

        Method queryLoaderMethod = testClass.getDeclaredMethod(suiteInfo.queryLoader(), String.class); 
        Method planLoaderMethod = testClass.getDeclaredMethod(suiteInfo.planLoader(), String.class); 

        queryLoader = queryId -> invoke(queryLoaderMethod, queryId);
        planLoader = queryId -> invoke(planLoaderMethod, queryId);

        CLUSTER = TestBuilders.cluster()
                .nodes("N1")
                .planningTimeout(1, TimeUnit.MINUTES)
                .build();
        CLUSTER.start();

        TestNode node = CLUSTER.node("N1");

        TpcTable[] tables = cast(suiteInfo.tables().getEnumConstants());
        for (TpcTable table : tables) {
            node.initSchema(table.ddlScript());

            CLUSTER.setTableSize(table.tableName().toUpperCase(), table.estimatedSize(TpcScaleFactor.SF_1GB));
        }
    }

    @AfterAll
    static void stopCluster() throws Exception {
        CLUSTER.stop();
    }

    static void validateQueryPlan(String queryId) {
        TestNode node = CLUSTER.node("N1");

        MultiStepPlan plan = (MultiStepPlan) node.prepare(queryLoader.apply(queryId));

        String actualPlan = RelOptUtil.toString(Cloner.clone(plan.root(), Commons.cluster()), SqlExplainLevel.DIGEST_ATTRIBUTES);
        String expectedPlan = planLoader.apply(queryId);

        assertEquals(expectedPlan, actualPlan);
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

    private static <T> T invoke(Method method, Object... arguments) {
        try {
            return (T) method.invoke(null, arguments);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    @Target(TYPE)
    @Retention(RUNTIME)
    public @interface TpcSuiteInfo {
        /** Returns enum representing set of tables to initialize for test. */ 
        Class<? extends Enum<? extends TpcTable>> tables();

        /**
         * Returns name of the method to use as query loader.
         *
         * <p>Specified method must be static method within class this annotation is specified upon.
         * Specified method must accept a single parameter of a string type which is query id, and return
         * string representing a query text.
         */
        String queryLoader();

        /**
         * Returns name of the method to use as plan loader.
         *
         * <p>Specified method must be static method within class this annotation is specified upon.
         * Specified method must accept a single parameter of a string type which is query id, and return
         * string representing a query plan.
         */
        String planLoader();
    }
}
