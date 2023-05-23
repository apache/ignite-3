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

package org.apache.ignite.internal.sql.engine.prepare.ddl;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Test;

/**
 * Tests the conversion of a ALTER TABLE ... ALTER COLUMN definition to a command.
 */
public class AlterColumnSqlToCommandConverterTest extends AbstractDdlSqlToCommandConverterTest {
    @Test
    public void testChangeColumnType() throws SqlParseException {
        SqlNode node = parse("ALTER TABLE T1 ALTER COLUMN C1 SET DATA TYPE INT");

        DdlCommand cmd = converter.convert((SqlDdl) node, createContext());
        assertThat(cmd, instanceOf(AlterColumnCommand.class));

//        AlterColumnCommand alterColCmd = (AlterColumnCommand) cmd;
    }
}
