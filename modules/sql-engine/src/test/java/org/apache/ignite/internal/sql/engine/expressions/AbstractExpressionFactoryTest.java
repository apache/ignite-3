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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.time.Instant;
import java.util.EnumSet;
import org.apache.ignite.internal.sql.engine.api.expressions.EvaluationContext;
import org.apache.ignite.internal.sql.engine.api.expressions.ExpressionFactory;
import org.apache.ignite.internal.sql.engine.api.expressions.RowAccessor;
import org.apache.ignite.internal.sql.engine.exec.exp.SqlExpressionFactoryImpl;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.EmptyCacheFactory;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.type.StructNativeType;
import org.apache.ignite.sql.ColumnType;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

abstract class AbstractExpressionFactoryTest {
    protected final ExpressionFactory factory = new SqlExpressionFactoryAdapter(
            new SqlExpressionFactoryImpl(
                    Commons.typeFactory(), 0, EmptyCacheFactory.INSTANCE
            )
    );

    protected EvaluationContext<Object[]> context() {
        return factory.<Object[]>contextBuilder()
                .rowAccessor(ObjectArrayAccessor.INSTANCE)
                .build();
    }

    protected EvaluationContext<Object[]> context(Instant currentTime) {
        return factory.<Object[]>contextBuilder()
                .rowAccessor(ObjectArrayAccessor.INSTANCE)
                .timeProvider(currentTime::toEpochMilli)
                .build();
    }

    protected static StructNativeType singleColumnType(NativeType type) {
        return NativeTypes.structBuilder()
                .addField("VAL", type, true)
                .build();
    }

    protected static @Nullable Object[] row(@Nullable Object value) {
        return new Object[] {value};
    }

    protected static class ObjectArrayAccessor implements RowAccessor<Object[]> {
        private static final ObjectArrayAccessor INSTANCE = new ObjectArrayAccessor();

        @Override
        public @Nullable Object get(int field, Object[] row) {
            return row[field];
        }

        @Override
        public int columnsCount(Object[] row) {
            return row.length;
        }

        @Override
        public boolean isNull(int field, Object[] row) {
            return row[field] == null;
        }
    }

    @Test
    void ensureAllMarshallableEntryTypesAreCovered() {
        EnumSet<ColumnType> typesToExclude = EnumSet.of(
                ColumnType.STRUCT, ColumnType.DURATION, ColumnType.PERIOD // are not fully supported yet.
        );

        EnumSet<ColumnType> missedTypes = EnumSet.complementOf(typesToExclude);

        for (Method method : this.getClass().getDeclaredMethods()) {
            if (!method.isAnnotationPresent(TestFor.class)) {
                continue;
            }

            TestFor type = method.getAnnotation(TestFor.class);

            missedTypes.remove(type.value());
        }

        assertThat("All column types must be covered. "
                + "Please add new test method for every missed column type using following template:\n\n"
                + "    @TestForType(ColumnType.MISSED_TYPE)\n"
                + "    void missedTypeTest() throws Exception {\n"
                + "        // do test\n"
                + "    }\n", missedTypes, empty());
    }

    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.RUNTIME)
    @interface TestFor {
        ColumnType value();
    }
}
