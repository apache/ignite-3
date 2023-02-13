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

package org.apache.ignite.internal.sql.engine.exec.exp;

import static org.apache.ignite.internal.sql.engine.util.BaseQueryContext.CALCITE_CONNECTION_CONFIG;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import java.util.Arrays;
import java.util.Collections;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.ignite.internal.sql.engine.rex.IgniteRexBuilder;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeSystem;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * ExpressionFactoryImpl test.
 */
public class ExpressionFactoryImplTest {
    /** Type factory. */
    private IgniteTypeFactory typeFactory;

    /** Expression factory. */
    private ExpressionFactoryImpl<Object[]> expFactory;

    @BeforeEach
    public void prepare() {
        RelDataTypeSystem typeSys = CALCITE_CONNECTION_CONFIG.typeSystem(RelDataTypeSystem.class, IgniteTypeSystem.INSTANCE);

        typeFactory = new IgniteTypeFactory(typeSys);

        expFactory = new ExpressionFactoryImpl<>(null, typeFactory, SqlConformanceEnum.DEFAULT);
    }

    @Test
    public void testScalarGeneration() {
        RelDataTypeField field = new RelDataTypeFieldImpl(
                "ID", 0, typeFactory.createSqlType(SqlTypeName.INTEGER)
        );
        RelRecordType type = new RelRecordType(Collections.singletonList(field));

        //Imagine we have 2 columns: (id: INTEGER, val: VARCHAR)
        RexDynamicParam firstNode = new RexDynamicParam(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);
        RexDynamicParam secondNode = new RexDynamicParam(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);

        SingleScalar scalar1 = expFactory.scalar(Arrays.asList(firstNode, secondNode), type);

        //Imagine we have 2 columns: (id: VARCHAR, val: INTEGER)
        firstNode = new RexDynamicParam(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0);
        secondNode = new RexDynamicParam(typeFactory.createSqlType(SqlTypeName.INTEGER), 1);

        SingleScalar scalar2 = expFactory.scalar(Arrays.asList(firstNode, secondNode), type);

        assertNotSame(scalar1, scalar2);
    }
}
