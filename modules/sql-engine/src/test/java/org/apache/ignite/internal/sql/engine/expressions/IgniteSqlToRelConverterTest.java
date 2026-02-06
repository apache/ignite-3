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

package org.apache.ignite.internal.sql.engine.expressions;

import static org.apache.ignite.internal.sql.engine.sql.IgniteSqlParser.PARSER_CONFIG;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.util.SourceStringReader;
import org.apache.ignite.internal.sql.engine.prepare.IgnitePlanner;
import org.apache.ignite.internal.sql.engine.prepare.PlanningContext;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.type.StructNativeType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Tests to validate conversion of expressions required for {@link SqlExpressionFactoryAdapter}.
 *
 * <p>Placed to package {@code org.apache.ignite.internal.sql.engine.expressions} to have access to package-private
 * classes {@link RowNamespace} and {@link RowBasedScope}.
 */
class IgniteSqlToRelConverterTest {
    private static final StructNativeType SINGLE_INT_COLUMN_STRUCT = NativeTypes.structBuilder()
            .addField("VAL", NativeTypes.INT32, false)
            .build();

    @ParameterizedTest
    @CsvSource({
            "ns_1.val, 0",
            "ns_2.val, 1",
            "ns_3.val, 2",
    })
    void multipleNamespacesTest(String expressionString, int expectedIndex) throws SqlParseException {
        // Given
        SqlNode expressionAst = parse(expressionString);

        try (IgnitePlanner planner = createPlanner()) {
            SqlValidator validator = planner.validator();
            RowBasedScope scope = new RowBasedScope(validator.getEmptyScope());

            RelDataType relType = TypeUtils.native2relationalType(planner.getTypeFactory(), SINGLE_INT_COLUMN_STRUCT);
            for (String name : List.of("NS_1", "NS_2", "NS_3")) {
                scope.addChild(new RowNamespace(validator, relType), name, false);
            }

            expressionAst.validateExpr(validator, scope);

            // When converting expression with multiple inputs
            RexNode rexNode = planner.sqlToRelConverter().convertExpressionExt(expressionAst, scope, relType, relType, relType);

            // Conversion must not throw exception and input reference must have correct index.
            assertThat(rexNode, instanceOf(RexInputRef.class));
            assertThat(((RexInputRef) rexNode).getIndex(), is(expectedIndex));
        }
    }

    private static SqlNode parse(String sql) throws SqlParseException {
        try (SourceStringReader reader = new SourceStringReader(sql)) {
            SqlParser parser = SqlParser.create(reader, PARSER_CONFIG);

            return parser.parseExpression();
        }
    }

    private static IgnitePlanner createPlanner() {
        return PlanningContext.builder().catalogVersion(-1).build().planner();
    }
}
