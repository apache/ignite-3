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

package org.apache.ignite.internal.cli.sql;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled("https://issues.apache.org/jira/browse/IGNITE-18956")
class SqlSchemaProviderTest {
    private static MetadataSupplier supplier;

    @BeforeAll
    public static void setup() throws SQLException {
        DatabaseMetaData meta = mock(DatabaseMetaData.class);

        when(meta.getTables(null, null, null, null)).thenReturn(mock(ResultSet.class));
        when(meta.getTables(any(), any(), any(), any())).thenReturn(mock(ResultSet.class));


        supplier = () -> meta;
    }

    @Test
    public void testProviderWithoutTimeout() throws InterruptedException {
        SqlSchemaProvider provider = new SqlSchemaProvider(supplier, 0);

        SqlSchema firstSchema = provider.getSchema();
        provider.getSchema(); // trigger update
        Thread.sleep(TimeUnit.SECONDS.toMillis(1));

        Assertions.assertNotEquals(firstSchema, provider.getSchema());
    }

    @Test
    public void testProviderWith1secTimeout() throws InterruptedException {
        SqlSchemaProvider provider = new SqlSchemaProvider(supplier, 1);

        provider.initStateAsync();
        Thread.sleep(TimeUnit.SECONDS.toMillis(1));

        SqlSchema schema = provider.getSchema();
        Thread.sleep(TimeUnit.SECONDS.toMillis(2));

        Assertions.assertNotEquals(schema, provider.getSchema());
    }
}
