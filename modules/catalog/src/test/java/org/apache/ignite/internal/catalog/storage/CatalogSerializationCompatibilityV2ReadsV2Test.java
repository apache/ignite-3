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

package org.apache.ignite.internal.catalog.storage;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.internal.catalog.storage.CatalogSerializationChecker.SerializerClass;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntryType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

/**
 * Tests for catalog storage objects. Protocol version 2 reads protocol 2.
 */
public class CatalogSerializationCompatibilityV2ReadsV2Test extends CatalogSerializationCompatibilityTest {

    private static final Set<SerializerClass> collected = new HashSet<>();

    @Override
    protected int protocolVersion() {
        return 2;
    }

    @Override
    protected int entryVersion() {
        return 2;
    }

    @Override
    protected String dirName() {
        return "serialization_v2";
    }

    @Override
    protected boolean expectExactVersion() {
        return true;
    }

    @Test
    public void alterTableProperties() {
        List<UpdateEntry> entries = List.of(
                new AlterTablePropertiesEntry(state.id(), null, null),
                new AlterTablePropertiesEntry(state.id(), 1.0d, null),
                new AlterTablePropertiesEntry(state.id(), null, 10L),
                new AlterTablePropertiesEntry(state.id(), 2.0d, 10L)
        );

        checker.addExpectedVersion(MarshallableEntryType.ALTER_TABLE_PROPERTIES.id(), 1);
        checker.compareEntries(entries, "AlterTableProperties", 1);
    }

    @AfterAll
    public static void allSerializersHaveTests() {
        // 1. Collect serializers (entry class + version)
        Set<SerializerClass> serializers = CatalogSerializationChecker.findEntrySerializers()
                .stream()
                .filter(sc -> {
                    // Exclude serializers for protocol version 1
                    return !SerializationV1Classes.includes(sc);
                })
                .collect(Collectors.toSet());

        // 2. Compare entry class + version with existing serializers
        compareSerializers(serializers, collected);
    }

    @Override
    protected void recordClass(SerializerClass clazz) {
        collected.add(clazz);
    }
}
