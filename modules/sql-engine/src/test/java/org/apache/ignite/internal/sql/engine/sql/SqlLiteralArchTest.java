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

package org.apache.ignite.internal.sql.engine.sql;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ArchRule;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;
import com.tngtech.archunit.lang.syntax.elements.GivenClassesConjunction;
import java.util.Arrays;
import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Litmus;

/**
 * {@link ArchRule ArchUnit rules} for subclasses of {@link SqlLiteral}.
 */
@AnalyzeClasses(
        packagesOf = IgniteSqlDecimalLiteral.class
)
public class SqlLiteralArchTest {

    @ArchTest
    @SuppressWarnings("unused")
    public static ArchRule LITERALS_IMPLEMENT_CLONE = sqlLiterals()
            .should(new ImplementMethod("clone", SqlParserPos.class));

    @ArchTest
    @SuppressWarnings("unused")
    public static ArchRule LITERALS_IMPLEMENT_CREATE_TYPE = sqlLiterals()
            .should(new ImplementMethod("createSqlType", RelDataTypeFactory.class));

    @ArchTest
    @SuppressWarnings("unused")
    public static ArchRule LITERALS_IMPLEMENT_UNPARSE = sqlLiterals()
            .should(new ImplementMethod("unparse", SqlWriter.class, int.class, int.class));

    @ArchTest
    @SuppressWarnings("unused")
    public static ArchRule LITERALS_IMPLEMENT_EQUALS_DEEP = sqlLiterals()
            .should(new ImplementMethod("equalsDeep", SqlNode.class, Litmus.class));


    /**
     * All subclasses of {@link SqlLiteral}.
     */
    private static GivenClassesConjunction sqlLiterals() {
        return classes().that().areAssignableTo(SqlLiteral.class);
    }

    /**
     * Checks that the given method is defined in a class.
     */
    private static class ImplementMethod extends ArchCondition<JavaClass> {

        private final String methodName;

        @SuppressWarnings("rawtypes")
        private final Class[] parameters;

        @SuppressWarnings("rawtypes")
        ImplementMethod(String methodName, Class... parameters) {
            super("have %s method defined", methodDisplayName(methodName, parameters));

            this.methodName = methodName;
            this.parameters = parameters;
        }

        /** {@inheritDoc} **/
        @Override
        public void check(JavaClass javaClass, ConditionEvents conditionEvents) {
            boolean satisfied;
            try {
                javaClass.getMethod(methodName, parameters);
                satisfied = true;
            } catch (IllegalArgumentException e) {
                satisfied = false;
            }

            var violation = javaClass.getFullName() + ": No method " + methodDisplayName(methodName, parameters);
            conditionEvents.add(new SimpleConditionEvent(javaClass, satisfied, violation));
        }

        @SuppressWarnings("rawtypes")
        private static String methodDisplayName(String methodName, Class[] parameters) {
            return format("{}({})", methodName, Arrays.stream(parameters).map(Class::getName).collect(Collectors.joining()));
        }
    }
}
