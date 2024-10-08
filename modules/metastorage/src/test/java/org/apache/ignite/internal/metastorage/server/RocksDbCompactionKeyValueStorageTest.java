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

package org.apache.ignite.internal.metastorage.server;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.ignite.internal.failure.NoOpFailureManager;
import org.apache.ignite.internal.metastorage.server.persistence.RocksDbKeyValueStorage;
import org.junit.jupiter.api.Test;

/** Compaction test for the RocksDB implementation of {@link KeyValueStorage}. */
public class RocksDbCompactionKeyValueStorageTest extends AbstractCompactionKeyValueStorageTest {
    @Override
    public KeyValueStorage createStorage() {
        return new RocksDbKeyValueStorage("test", workDir.resolve("storage"), new NoOpFailureManager());
    }

    @Test
    void checksumsAreRemovedForCompactedRevisions() {
        assertNotNull(storage.checksum(3));

        storage.compact(3);

        assertNull(storage.checksum(1));
        assertNull(storage.checksum(2));
        assertNull(storage.checksum(3));
        assertNotNull(storage.checksum(4));
    }
}
