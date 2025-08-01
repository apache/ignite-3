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

package org.apache.ignite.internal.catalog.sql;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.apache.ignite.table.QualifiedName;
import org.junit.jupiter.api.Test;

class DropTableTest {
    @Test
    void testDropTable() {
        Query query1 = dropTable().name(QualifiedName.parse("table1"));
        String sql = query1.toString();
        assertThat(sql, is("DROP TABLE PUBLIC.TABLE1;"));

        Query query2 = dropTable().ifExists().name(QualifiedName.parse("table1"));
        assertThat(query2.toString(), is("DROP TABLE IF EXISTS PUBLIC.TABLE1;"));

        Query query3 = dropTable().ifExists().name(QualifiedName.parse("\"a b\""));
        sql = query3.toString();
        assertThat(sql, is("DROP TABLE IF EXISTS PUBLIC.\"a b\";"));

        Query query4 = dropTable().name(QualifiedName.parse("\"MyTable\""));
        sql = query4.toString();
        assertThat(sql, is("DROP TABLE PUBLIC.\"MyTable\";"));
    }

    private static DropTableImpl dropTable() {
        return new DropTableImpl(null);
    }
}
