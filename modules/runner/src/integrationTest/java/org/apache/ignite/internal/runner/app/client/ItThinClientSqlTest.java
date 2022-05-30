/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.runner.app.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.async.AsyncResultSet;
import org.junit.jupiter.api.Test;

/**
 * Thin client SQL integration test.
 */
public class ItThinClientSqlTest extends ItAbstractThinClientTest {
    @Test
    void testExecuteAsyncSimple() {
        AsyncResultSet resultSet = client().sql()
                .createSession()
                .executeAsync(null, "select 1 as num, 'hello' as str")
                .join();

        assertTrue(resultSet.hasRowSet());
        assertFalse(resultSet.wasApplied());
        assertEquals(1, resultSet.currentPageSize());

        SqlRow row = resultSet.currentPage().iterator().next();
        assertEquals(1, row.intValue(0));
        assertEquals("hello", row.stringValue(1));

        List<ColumnMetadata> columns = resultSet.metadata().columns();
        assertEquals(2, columns.size());
        assertEquals("NUM", columns.get(0).name());
        assertEquals("STR", columns.get(1).name());
    }
}
