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

import static org.apache.ignite.internal.sql.engine.util.Commons.FRAMEWORK_CONFIG;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.assertThrowsSqlException;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.Frameworks;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.prepare.ddl.DdlSqlToCommandConverter;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.sql.ParsedResult;
import org.apache.ignite.internal.sql.engine.sql.ParserServiceImpl;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.util.BaseQueryContext;
import org.apache.ignite.internal.sql.engine.util.SqlTestUtils;
import org.apache.ignite.internal.sql.engine.util.cache.CaffeineCacheFactory;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests to verify {@link PrepareServiceImpl}.
 */
@SuppressWarnings("DataFlowIssue")
public class PrepareServiceImplTest extends BaseIgniteAbstractTest {
    private static final List<PrepareService> createdServices = new ArrayList<>();

    @AfterEach
    public void stopServices() throws Exception {
        for (PrepareService createdService : createdServices) {
            createdService.stop();
        }

        createdServices.clear();
    }

    @Test
    public void prepareServiceReturnsExistingPlanForExplain() {
        PrepareService service = createPlannerService();

        QueryPlan queryPlan = await(service.prepareAsync(
                parse("SELECT * FROM t"),
                createContext()
        ));

        QueryPlan explainPlan = await(service.prepareAsync(
                parse("explain plan for select * from t"),
                createContext()
        ));

        assertThat(explainPlan, instanceOf(ExplainPlan.class));

        ExplainPlan plan = (ExplainPlan) explainPlan;

        assertThat(plan.plan(), sameInstance(queryPlan));
    }

    @Test
    public void prepareServiceCachesPlanCreatedForExplain() {
        PrepareService service = createPlannerService();

        QueryPlan explainPlan = await(service.prepareAsync(
                parse("explain plan for select * from t"),
                createContext()
        ));

        QueryPlan queryPlan = await(service.prepareAsync(
                parse("SELECT * FROM t"),
                createContext()
        ));

        assertThat(explainPlan, instanceOf(ExplainPlan.class));

        ExplainPlan plan = (ExplainPlan) explainPlan;

        assertThat(plan.plan(), sameInstance(queryPlan));
    }

    @Test
    public void prepareReturnsQueryPlanThatDependsOnParameterTypeMatchInferred() {
        PrepareService service = createPlannerService();

        QueryPlan queryPlan1 = await(service.prepareAsync(
                parse("SELECT * FROM t WHERE a = ? and c = ?"),
                createContext()
        ));

        List<ColumnType> parameterTypes = queryPlan1.parameterMetadata().parameterTypes()
                .stream()
                .map(ParameterType::columnType)
                .collect(Collectors.toList());

        assertEquals(List.of(ColumnType.INT64, ColumnType.INT32), parameterTypes);

        // Parameter types match, we should return plan1.
        QueryPlan queryPlan2 = await(service.prepareAsync(
                parse("SELECT * FROM t WHERE a = ? and c = ?"),
                createContext(1L, 1))
        );
        assertSame(queryPlan1, queryPlan2);

        // Parameter types do not match
        QueryPlan queryPlan3 = await(service.prepareAsync(
                parse("SELECT * FROM t WHERE a = ? and c = ?"),
                createContext(1, 1L)
        ));
        assertNotSame(queryPlan1, queryPlan3);
    }

    @Test
    public void prepareReturnsDmlPlanThatDependsOnParameterTypeMatchInferred() {
        PrepareService service = createPlannerService();

        QueryPlan queryPlan1 = await(service.prepareAsync(
                parse("UPDATE t SET a = ? WHERE c = ?"),
                createContext()
        ));

        List<ColumnType> parameterTypes = queryPlan1.parameterMetadata().parameterTypes()
                .stream()
                .map(ParameterType::columnType)
                .collect(Collectors.toList());

        assertEquals(List.of(ColumnType.INT64, ColumnType.INT32), parameterTypes);

        // Parameter types match, we should return plan1.
        QueryPlan queryPlan2 = await(service.prepareAsync(
                parse("UPDATE t SET a = ? WHERE c = ?"),
                createContext(1L, 1)
        ));
        assertSame(queryPlan1, queryPlan2);

        // Parameter types do not match
        QueryPlan queryPlan3 = await(service.prepareAsync(
                parse("UPDATE t SET a = ? WHERE c = ?"),
                createContext(1, 1L)
        ));
        assertNotSame(queryPlan1, queryPlan3);
    }

    @Test
    public void preparePropagatesValidationError() {
        PrepareService service = createPlannerService();

        assertThrowsSqlException(Sql.STMT_VALIDATION_ERR,
                "Ambiguous operator <UNKNOWN> + <UNKNOWN>. Dynamic parameter requires adding explicit type cast",
                () -> {
                    ParsedResult parsedResult = parse("SELECT ? + ?");
                    BaseQueryContext context = createContext();
                    await(service.prepareAsync(parsedResult, context));
                }
        );
    }

    @ParameterizedTest
    @MethodSource("parameterTypes")
    public void prepareParamInPredicateAllTypes(NativeType nativeType, int precision, int scale) {
        IgniteTable table = TestBuilders.table()
                .name("T")
                .addColumn("C", nativeType)
                .distribution(IgniteDistributions.single())
                .build();

        PrepareService service = createPlannerService();

        IgniteSchema schema = new IgniteSchema("PUBLIC", 0, List.of(table));
        Object paramValue = SqlTestUtils.generateValueByType(nativeType.spec().asColumnType());

        QueryPlan queryPlan = await(service.prepareAsync(
                parse("SELECT * FROM t WHERE c = ?"),
                createContext(schema, paramValue)
        ));

        ParameterType parameterType = queryPlan.parameterMetadata().parameterTypes().get(0);

        ColumnType columnType = nativeType.spec().asColumnType();
        assertEquals(columnType, parameterType.columnType(), "Column type does not match: " + parameterType);
        assertEquals(precision, parameterType.precision(), "Precision does not match: " + parameterType);
        assertEquals(scale, parameterType.scale(), "Scale does not match: " + parameterType);
        assertTrue(parameterType.nullable(), "Nullabilty does not match: " + parameterType);
    }

    private static Stream<Arguments> parameterTypes() {
        int noScale = ColumnMetadata.UNDEFINED_SCALE;
        int noPrecision = ColumnMetadata.UNDEFINED_PRECISION;

        return Stream.of(
                Arguments.of(NativeTypes.BOOLEAN, noPrecision, noScale),
                Arguments.of(NativeTypes.INT8, noPrecision, noScale),
                Arguments.of(NativeTypes.INT16, noPrecision, noScale),
                Arguments.of(NativeTypes.INT32, noPrecision, noScale),
                Arguments.of(NativeTypes.INT64, noPrecision, noScale),
                Arguments.of(NativeTypes.FLOAT, noPrecision, noScale),
                Arguments.of(NativeTypes.DOUBLE, noPrecision, noScale),
                Arguments.of(NativeTypes.decimalOf(10, 2), Short.MAX_VALUE, 0),
                Arguments.of(NativeTypes.stringOf(42), -1, noScale),
                Arguments.of(NativeTypes.blobOf(42), -1, noScale),
                Arguments.of(NativeTypes.UUID, noPrecision, noScale),
                Arguments.of(NativeTypes.DATE, noPrecision, noScale),
                Arguments.of(NativeTypes.time(2), 0, noScale),
                Arguments.of(NativeTypes.datetime(2), 6, noScale),
                Arguments.of(NativeTypes.timestamp(2), 6, noScale)
        );
    }

    private static ParsedResult parse(String query) {
        return new ParserServiceImpl().parse(query);
    }

    private static BaseQueryContext createContext(Object... params) {
        return BaseQueryContext.builder()
                .queryId(UUID.randomUUID())
                .frameworkConfig(
                        Frameworks.newConfigBuilder(FRAMEWORK_CONFIG)
                                .defaultSchema(wrap(createSchema()))
                                .build()
                )
                .parameters(params)
                .build();
    }

    private static BaseQueryContext createContext(IgniteSchema schema, Object... params) {
        return BaseQueryContext.builder()
                .queryId(UUID.randomUUID())
                .frameworkConfig(
                        Frameworks.newConfigBuilder(FRAMEWORK_CONFIG)
                                .defaultSchema(wrap(schema))
                                .build()
                )
                .parameters(params)
                .build();
    }

    private static SchemaPlus wrap(IgniteSchema schema) {
        var schemaPlus = Frameworks.createRootSchema(false);

        schemaPlus.add(schema.getName(), schema);

        return schemaPlus.getSubSchema(schema.getName());
    }

    private static IgniteSchema createSchema() {
        IgniteTable table = TestBuilders.table()
                .name("T")
                .addColumn("A", NativeTypes.INT64)
                .addColumn("C", NativeTypes.INT32)
                .distribution(IgniteDistributions.single())
                .build();

        return new IgniteSchema("PUBLIC", 0, List.of(table));
    }

    private static PrepareService createPlannerService() {
        PrepareService service = new PrepareServiceImpl("test", 1_000, CaffeineCacheFactory.INSTANCE,
                mock(DdlSqlToCommandConverter.class), 5_000, 2, mock(MetricManager.class));

        createdServices.add(service);

        service.start();

        return service;
    }
}
