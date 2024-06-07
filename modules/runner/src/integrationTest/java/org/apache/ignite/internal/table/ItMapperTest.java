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

package org.apache.ignite.internal.table;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.Objects;
import org.apache.ignite.catalog.ColumnType;
import org.apache.ignite.catalog.definitions.ColumnDefinition;
import org.apache.ignite.catalog.definitions.TableDefinition;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.table.mapper.TypeConverter;
import org.junit.jupiter.api.Test;

/**
 * POJO mappers test.
 */
public class ItMapperTest extends ClusterPerClassIntegrationTest {
    @Test
    void valueWithTypeConverter() throws Exception {
        IgniteImpl node = CLUSTER.node(0);

        var tableDef = TableDefinition.builder("PrimTestTable")
                .columns(
                        ColumnDefinition.column("ID", ColumnType.INT32),
                        ColumnDefinition.column("VAL", ColumnType.DATE)
                )
                .primaryKey("ID")
                .build();

        node.catalog().createTableAsync(tableDef).get();

        var keyMapper = Mapper.of(Integer.class);
        var valMapper = Mapper.of(Date.class, "VAL", new DateTypeConverter());
        var table = node.tables().table("PrimTestTable").keyValueView(keyMapper, valMapper);

        var expected = Date.from(LocalDate.now().atStartOfDay().toInstant(ZoneOffset.UTC));
        table.put(null, 1, expected);
        var actual = table.get(null, 1);

        assertEquals(expected, actual);
    }

    @Test
    void pojoWithTypeConverter() throws Exception {
        IgniteImpl node = CLUSTER.node(0);

        var tableDef = TableDefinition.builder("PojoTestTable")
                .key(Integer.class)
                .value(MyPojoWithDate.class)
                .build();

        node.catalog().createTableAsync(tableDef).get();

        var keyMapper = Mapper.of(Integer.class);
        var valMapper = Mapper.builder(MyPojoWithDate.class)
                .map("name", "NAME")
                .map("birthday", "BIRTHDAY", new DateTypeConverter())
                .build();

        var table = node.tables().table("PojoTestTable").keyValueView(keyMapper, valMapper);

        var expectedDate = Date.from(LocalDate.now().atStartOfDay().toInstant(ZoneOffset.UTC));
        var expected = new MyPojoWithDate("someName", expectedDate);

        table.put(null, 1, expected);
        var actual = table.get(null, 1);

        assertEquals(expected, actual);
    }

    private static class DateTypeConverter implements TypeConverter<Date, LocalDate> {
        @Override
        public LocalDate toColumnType(Date obj) {
            var timestamp = obj.toInstant();
            return LocalDate.ofInstant(timestamp, ZoneOffset.UTC);
        }

        @Override
        public Date toObjectType(LocalDate data) {
            Instant instant = data.atStartOfDay().toInstant(ZoneOffset.UTC);
            return Date.from(instant);
        }
    }

    static class MyPojoWithDate {
        private String name;

        private Date birthday;

        public MyPojoWithDate() {
            // No-op.
        }

        public MyPojoWithDate(String name, Date birthday) {
            this.name = name;
            this.birthday = birthday;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            MyPojoWithDate that = (MyPojoWithDate) o;
            return Objects.equals(name, that.name) && Objects.equals(birthday, that.birthday);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, birthday);
        }
    }
}
