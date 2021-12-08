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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.junit.jupiter.api.Test;

/**
 * Record view tests.
 */
public class ClientRecordViewTest extends AbstractClientTableTest {
    @Test
    public void testBinaryPutPojoGet() {
        Table table = defaultTable();
        RecordView<PersonPojo> pojoView = table.recordView(Mapper.of(PersonPojo.class));

        table.recordView().upsert(tuple());

        var key = new PersonPojo();
        key.id = DEFAULT_ID;

        PersonPojo val = pojoView.get(key);
        PersonPojo missingVal = pojoView.get(new PersonPojo());

        assertEquals(DEFAULT_NAME, val.name);
        assertEquals(DEFAULT_ID, val.id);
        assertNull(missingVal);
    }

    @Test
    public void testBinaryPutPrimitiveGet() {
        Table table = defaultTable();
        RecordView<Long> primitiveView = table.recordView(Mapper.of(Long.class));

        table.recordView().upsert(tuple());

        Long val = primitiveView.get(DEFAULT_ID);
        Long missingVal = primitiveView.get(-1L);

        assertEquals(DEFAULT_ID, val);
        assertNull(missingVal);
    }

    @Test
    public void testPojoPutBinaryGet() {
        // TODO
    }

    @Test
    public void testPrimitivePutBinaryGet() {
        Table table = oneColumnTable();
        RecordView<String> primitiveView = table.recordView(Mapper.of(String.class));

        primitiveView.upsert("abc");

        Tuple tuple = table.recordView().get(oneColumnTableKey("abc"));
        assertEquals("abc", tuple.stringValue(0));
    }

    @Test
    public void testMissingValueColumnsAreSkipped() {
        Table table = fullTable();
        KeyValueView<Tuple, Tuple> kvView = table.keyValueView();
        RecordView<IncompletePojo> pojoView = table.recordView(IncompletePojo.class);

        kvView.put(allClumnsTableKey(1), allColumnsTableVal("x"));

        var key = new IncompletePojo();
        key.id = "1";
        key.gid = 1;

        // This POJO does not have fields for all table columns, and this is ok.
        IncompletePojo val = pojoView.get(key);

        assertEquals(1, val.gid);
        assertEquals("1", val.id);
        assertEquals("x", val.zstring);
        assertEquals(2, val.zbytes[1]);
        assertEquals(11, val.zbyte);
    }

    @Test
    public void testAllColumnsBinaryPutPojoGet() {
        Table table = fullTable();
        RecordView<AllColumnsPojo> pojoView = table.recordView(Mapper.of(AllColumnsPojo.class));

        table.recordView().upsert(allColumnsTableVal("foo"));

        var key = new AllColumnsPojo();
        key.gid = (int)(long)DEFAULT_ID;
        key.id = String.valueOf(DEFAULT_ID);

        AllColumnsPojo res = pojoView.get(key);
        assertEquals(1, res.zint); // TODO: All asserts
    }

    @Test
    public void testAllColumnsPojoPutBinaryGet() {
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

    private static class IncompletePojo {
        public byte zbyte;
        public String id;
        public int gid;
        public String zstring;
        public byte[] zbytes;
    }

    private static class AllColumnsPojo {
        public int gid;
        public String id;
        public byte zbyte;
        public short zshort;
        public int zint;
        public long zlong;
        public float zfloat;
        public double zdouble;
        public LocalDate zdate;
        public LocalTime ztime;
        public Instant ztimestamp;
        public String zstring;
        public byte[] zbytes;
        public UUID zuuid;
        public BitSet zbitmask;
        public BigDecimal zdecimal;
        public BigInteger znumber;
    }
}
