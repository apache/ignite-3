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

package org.apache.ignite.internal.sql.engine.datatypes.uuid;

import java.util.UUID;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.sql.engine.datatypes.DataTypeTestSpecs;
import org.apache.ignite.internal.sql.engine.datatypes.tests.BaseJoinDataTypeTest;
import org.apache.ignite.internal.sql.engine.datatypes.tests.DataTypeTestSpec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;

/**
 * Tests for {@code JOIN} operator for {@link SqlTypeName#UUID UUID data type}.
 */
public class ItUuidJoinTest extends BaseJoinDataTypeTest<UUID> {
    /** {@inheritDoc} */
    @Override
    protected DataTypeTestSpec<UUID> getTypeSpec() {
        return DataTypeTestSpecs.UUID_TYPE;
    }

    @Override
    @AfterEach
    public void cleanJoinTables() {
        runSql("DELETE FROM t_join_uuid");
        runSql("DELETE FROM t_join_varchar");
    }

    /** Creates join tables. */
    @Override
    @BeforeAll
    public void createJoinTables() {
        runSql("create table t_join_uuid(id integer primary key, test_key uuid)");
        runSql("create table t_join_varchar(id integer primary key, test_key varchar)");
    }
}
