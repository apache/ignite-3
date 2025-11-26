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

package org.apache.ignite.client;

import static org.apache.ignite.client.fakes.FakeIgniteTables.TABLE_ALL_COLUMNS;
import static org.apache.ignite.client.fakes.FakeIgniteTables.TABLE_ONE_COLUMN;
import static org.apache.ignite.client.fakes.FakeIgniteTables.TABLE_WITH_DEFAULT_VALUES;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.client.fakes.FakeIgniteTables;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.AssertionFailureBuilder;

/**
 * Base class for client table tests.
 */
public class AbstractClientTableTest extends AbstractClientTest {
    /** Default name. */
    protected static final String DEFAULT_NAME = "John";

    /** Default id. */
    protected static final Long DEFAULT_ID = 123L;

    protected static final LocalDate localDate = LocalDate.now();

    protected static final LocalTime localTime = LocalTime.now();

    protected static final Instant instant = Instant.now();

    protected static final UUID uuid = UUID.randomUUID();

    protected static Tuple[] sortedTuples(Collection<Tuple> tuples) {
        Tuple[] res = tuples.toArray(new Tuple[0]);

        Arrays.sort(res, (x, y) -> (int) (x.longValue(0) - y.longValue(0)));

        return res;
    }

    static Tuple tuple() {
        return Tuple.create()
                .set("id", DEFAULT_ID)
                .set("name", DEFAULT_NAME);
    }

    static Tuple tuple(Long id) {
        return Tuple.create()
                .set("id", id);
    }

    static Tuple tuple(Long id, String name) {
        return Tuple.create()
                .set("id", id)
                .set("name", name);
    }

    static Tuple defaultTupleKey() {
        return Tuple.create().set("id", DEFAULT_ID);
    }

    static Tuple tupleKey(long id) {
        return Tuple.create().set("id", id);
    }

    protected static Tuple tupleVal(String name) {
        return Tuple.create().set("name", name);
    }

    protected static void assertDecimalEqual(BigDecimal expected, BigDecimal actual) {
        if (expected == null && actual == null) {
            return;
        }

        if (expected != null && actual != null && expected.compareTo(actual) == 0) {
            return;
        }

        AssertionFailureBuilder.assertionFailure().expected(expected).actual(actual).buildAndThrow();
    }

    protected Table defaultTable() {
        if (server.tables().table(DEFAULT_TABLE) == null) {
            ((FakeIgniteTables) server.tables()).createTable(DEFAULT_TABLE);
        }

        return client.tables().table(DEFAULT_TABLE);
    }

    protected Table tableWithDefaultValues() {
        if (server.tables().table(TABLE_WITH_DEFAULT_VALUES) == null) {
            ((FakeIgniteTables) server.tables()).createTable(TABLE_WITH_DEFAULT_VALUES);
        }

        return client.tables().table(TABLE_WITH_DEFAULT_VALUES);
    }

    protected static Tuple allColumnsTableKey(long id) {
        return Tuple.create().set("gid", id).set("id", String.valueOf(id));
    }

    protected static Tuple allColumnsTableVal(String name, boolean skipKey) {
        var tuple = Tuple.create()
                .set("zboolean", true)
                .set("zbyte", (byte) 11)
                .set("zshort", (short) 12)
                .set("zint", 13)
                .set("zlong", (long) 14)
                .set("zfloat", (float) 1.5)
                .set("zdouble", 1.6)
                .set("zdate", localDate)
                .set("ztime", localTime)
                .set("ztimestamp", instant)
                .set("zstring", name)
                .set("zbytes", new byte[]{1, 2})
                .set("zdecimal", BigDecimal.valueOf(21))
                .set("zuuid", uuid);

        if (!skipKey) {
            tuple
                    .set("gid", DEFAULT_ID)
                    .set("id", String.valueOf(DEFAULT_ID));
        }

        return tuple;
    }

    protected Table fullTable() {
        if (server.tables().table(TABLE_ALL_COLUMNS) == null) {
            ((FakeIgniteTables) server.tables()).createTable(TABLE_ALL_COLUMNS);
        }

        return client.tables().table(TABLE_ALL_COLUMNS);
    }

    protected static Tuple oneColumnTableKey(String id) {
        return Tuple.create().set("id", id);
    }

    protected Table oneColumnTable() {
        if (server.tables().table(TABLE_ONE_COLUMN) == null) {
            ((FakeIgniteTables) server.tables()).createTable(TABLE_ONE_COLUMN);
        }

        return client.tables().table(TABLE_ONE_COLUMN);
    }

    /** Person. */
    protected static class PersonPojo {
        public long id;

        public String name;

        public PersonPojo() {
            // No-op.
        }

        public PersonPojo(long id) {
            this.id = id;
        }

        public PersonPojo(long id, String name) {
            this.id = id;
            this.name = name;
        }
    }

    /** Person without key. */
    protected static class PersonValPojo {
        public String name;

        public PersonValPojo() {
            // No-op.
        }

        public PersonValPojo(String name) {
            this.name = name;
        }
    }

    /** Name column. */
    protected static class NamePojo {
        public String name;
    }

    /** Partial column set. */
    protected static class IncompletePojo {
        public byte zbyte;
        public String id;
        public long gid;
        public String zstring;
        public byte[] zbytes;
    }

    /** Composite key. */
    protected static class CompositeKeyPojo {
        public String id;
        public long gid;
    }

    /** Partial column set. */
    protected static class IncompleteValPojo {
        public byte zbyte;
        public String zstring;
        public byte[] zbytes;
    }

    /** Columns of all types. */
    protected static class AllColumnsPojo {
        public long gid;
        public String id;
        public boolean zboolean;
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
        public BigDecimal zdecimal;
    }

    /** Columns of all types. */
    protected static class AllColumnsPojoNullable {
        public Long gid;
        public String id;
        public Boolean zboolean;
        public Byte zbyte;
        public Short zshort;
        public Integer zint;
        public Long zlong;
        public Float zfloat;
        public Double zdouble;
        public LocalDate zdate;
        public LocalTime ztime;
        public Instant ztimestamp;
        public String zstring;
        public byte[] zbytes;
        public UUID zuuid;
        public BigDecimal zdecimal;
    }

    /** Columns of all types. */
    protected static class AllColumnsValPojo {
        public boolean zboolean;
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
        public BigDecimal zdecimal;
    }

    /** Columns of all types. */
    protected static class AllColumnsValPojoNullable {
        public Boolean zboolean;
        public Byte zbyte;
        public Short zshort;
        public Integer zint;
        public Long zlong;
        public Float zfloat;
        public Double zdouble;
        public LocalDate zdate;
        public LocalTime ztime;
        public Instant ztimestamp;
        public String zstring;
        public byte[] zbytes;
        public UUID zuuid;
        public BigDecimal zdecimal;
    }

    /** Columns with default values. */
    protected static class DefaultValuesPojo {
        public int id;
        public String str;
        public String strNonNull;
        public Byte num;
    }

    /** Columns with default values. */
    protected static class DefaultValuesValPojo {
        public String str;
        public String strNonNull;
        public Byte num;
    }
}
