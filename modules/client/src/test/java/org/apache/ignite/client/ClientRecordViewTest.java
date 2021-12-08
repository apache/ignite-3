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

package org.apache.ignite.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.concurrent.CompletionException;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.mapper.Mapper;
import org.junit.jupiter.api.Test;

/**
 * Record view tests.
 */
public class ClientRecordViewTest extends AbstractClientTableTest {
    @Test
    public void testBinaryUpsertPojoGet() {
        Table table = defaultTable();
        table.recordView().upsert(tuple());

        RecordView<PersonPojo> pojoView = table.recordView(Mapper.of(PersonPojo.class));

        var key = new PersonPojo();
        key.id = DEFAULT_ID;

        PersonPojo val = pojoView.get(key);
        PersonPojo missingVal = pojoView.get(new PersonPojo());

        assertEquals(DEFAULT_NAME, val.name);
        assertEquals(DEFAULT_ID, val.id);
        assertNull(missingVal);
    }

    @Test
    public void testBinaryUpsertPrimitiveGet() {
        Table table = defaultTable();
        table.recordView().upsert(tuple());

        RecordView<Long> primitiveView = table.recordView(Mapper.of(Long.class));

        Long val = primitiveView.get(DEFAULT_ID);
        Long missingVal = primitiveView.get(-1L);

        assertEquals(DEFAULT_ID, val);
        assertNull(missingVal);
    }

    @Test
    public void testPojoUpsertBinaryGet() {
        // TODO
    }

    @Test
    public void testPrimitiveUpsertBinaryGet() {
        // TODO
    }

    @Test
    public void testMissingValueColumnIsIgnored() {
        // TODO
    }

    @Test
    public void testMissingKeyColumnThrowsException() {
        RecordView<NamePojo> recordView = defaultTable().recordView(NamePojo.class);

        CompletionException e = assertThrows(CompletionException.class, () -> recordView.get(new NamePojo()));
        IgniteClientException ice = (IgniteClientException) e.getCause();

        assertEquals("No field found for column id", ice.getMessage());
    }

    private static class PersonPojo {
        public long id;

        public String name;
    }

    private static class IdPojo {
        public long id;
    }

    private static class NamePojo {
        public String name;
    }
}
