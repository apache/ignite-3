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

package org.apache.ignite.internal.vault.persistence;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.vault.VaultEntry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Test suite for testing persistence properties of {@link PersistentVaultService}.
 */
@ExtendWith(WorkDirectoryExtension.class)
class ItPersistencePropertiesVaultServiceTest {
    @WorkDirectory
    private Path vaultDir;

    /**
     * Tests that the Vault Service correctly persists data after multiple service restarts.
     */
    @Test
    void testPersistentRestart() {
        var data = Map.of(
                new ByteArray("key" + 1), fromString("value" + 1),
                new ByteArray("key" + 2), fromString("value" + 2),
                new ByteArray("key" + 3), fromString("value" + 3)
        );

        var service = new PersistentVaultService(vaultDir);

        try {
            service.start();

            service.putAll(data);
        } finally {
            service.close();
        }

        service = new PersistentVaultService(vaultDir);

        try {
            service.start();

            assertThat(
                    service.get(new ByteArray("key" + 1)),
                    is(equalTo(new VaultEntry(new ByteArray("key" + 1), fromString("value" + 1))))
            );
        } finally {
            service.close();
        }

        service = new PersistentVaultService(vaultDir);

        try {
            service.start();

            try (var cursor = service.range(new ByteArray("key" + 1), new ByteArray("key" + 4))) {
                List<VaultEntry> expectedData = data.entrySet().stream()
                        .map(e -> new VaultEntry(e.getKey(), e.getValue()))
                        .sorted(Comparator.comparing(VaultEntry::key))
                        .collect(toList());

                assertThat(cursor.stream().collect(toList()), is(expectedData));
            }
        } finally {
            service.close();
        }
    }

    /**
     * Converts a {@code String} into a byte array.
     */
    private static byte[] fromString(String str) {
        return str.getBytes(StandardCharsets.UTF_8);
    }
}
