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
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.internal.catalog.storage.CatalogSerializationChecker.SerializerClass;
import org.junit.jupiter.api.AfterAll;

/**
 * Tests for catalog storage objects. Protocol version 1 read v1.
 */
public class CatalogSerializationCompatibilityV1ReadsV1Test extends CatalogSerializationCompatibilityTest {

    private static final Set<SerializerClass> collected = new HashSet<>();

    @Override
    protected int protocolVersion() {
        return 1;
    }

    @Override
    protected int entryVersion() {
        return 1;
    }

    @Override
    protected String dirName() {
        return "serialization_v1";
    }

    @Override
    protected boolean expectExactVersion() {
        return true;
    }

    @AfterAll
    public static void allSerializersHaveTests() {
        // 1. Collect serializers (entry class + version)x
        Set<SerializerClass> serializers = CatalogSerializationChecker.findEntrySerializers()
                .stream()
                // Serializers for protocol version 1.
                .filter(SerializationV1Classes::includes)
                .collect(Collectors.toSet());

        // 2. Compare entry class + version with existing serializers
        compareSerializers(serializers, collected);
    }

    @Override
    protected void recordClass(SerializerClass clazz) {
        collected.add(clazz);
    }
}
