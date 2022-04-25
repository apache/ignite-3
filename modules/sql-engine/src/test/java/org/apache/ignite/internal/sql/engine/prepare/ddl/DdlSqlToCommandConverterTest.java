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

package org.apache.ignite.internal.sql.engine.prepare.ddl;

import static org.apache.ignite.internal.sql.engine.prepare.ddl.DdlSqlToCommandConverter.checkDuplicates;
import static org.apache.ignite.internal.sql.engine.prepare.ddl.DdlSqlToCommandConverter.collectDataStorageNames;
import static org.apache.ignite.internal.sql.engine.prepare.ddl.DdlSqlToCommandConverter.collectTableOptionInfos;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * For {@link DdlSqlToCommandConverter} testing.
 */
public class DdlSqlToCommandConverterTest {
    @Test
    void testCollectDataStorageNames() {
        assertThat(collectDataStorageNames(Set.of()), equalTo(Map.of()));

        assertThat(
                collectDataStorageNames(Set.of("rocksdb")),
                equalTo(Map.of("ROCKSDB", "rocksdb"))
        );

        assertThat(
                collectDataStorageNames(Set.of("ROCKSDB")),
                equalTo(Map.of("ROCKSDB", "ROCKSDB"))
        );

        assertThat(
                collectDataStorageNames(Set.of("rocksDb", "pageMemory")),
                equalTo(Map.of("ROCKSDB", "rocksDb", "PAGEMEMORY", "pageMemory"))
        );

        IllegalStateException exception = assertThrows(
                IllegalStateException.class,
                () -> collectDataStorageNames(Set.of("rocksdb", "rocksDb"))
        );

        assertThat(exception.getMessage(), startsWith("Duplicate key"));
    }

    @Test
    void testCollectTableOptionInfos() {
        assertThat(collectTableOptionInfos(new TableOptionInfo[0]), equalTo(Map.of()));

        TableOptionInfo<?> replicas = tableOptionInfo("replicas");

        assertThat(
                collectTableOptionInfos(replicas),
                equalTo(Map.of("REPLICAS", replicas))
        );

        replicas = tableOptionInfo("REPLICAS");

        assertThat(
                collectTableOptionInfos(replicas),
                equalTo(Map.of("REPLICAS", replicas))
        );

        replicas = tableOptionInfo("replicas");
        TableOptionInfo<?> partitions = tableOptionInfo("partitions");

        assertThat(
                collectTableOptionInfos(replicas, partitions),
                equalTo(Map.of("REPLICAS", replicas, "PARTITIONS", partitions))
        );

        TableOptionInfo<?> replicas0 = tableOptionInfo("replicas");
        TableOptionInfo<?> replicas1 = tableOptionInfo("REPLICAS");

        IllegalStateException exception = assertThrows(
                IllegalStateException.class,
                () -> collectTableOptionInfos(replicas0, replicas1)
        );

        assertThat(exception.getMessage(), startsWith("Duplicate key"));
    }

    @Test
    void testCheckPositiveNumber() {
        IllegalStateException exception = assertThrows(
                IllegalStateException.class,
                () -> checkDuplicates(
                        collectTableOptionInfos(tableOptionInfo("replicas")),
                        collectTableOptionInfos(tableOptionInfo("replicas"))
                )
        );

        assertThat(exception.getMessage(), startsWith("Duplicate id"));

        assertDoesNotThrow(() -> checkDuplicates(
                collectTableOptionInfos(tableOptionInfo("replicas")),
                collectTableOptionInfos(tableOptionInfo("partitions"))
        ));
    }

    private TableOptionInfo tableOptionInfo(String name) {
        return new TableOptionInfo<>(name, Object.class, null, (createTableCommand, o) -> {
        });
    }
}
