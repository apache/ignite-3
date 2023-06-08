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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for {@link TestRocksDbKeyValueStorage} key-value storage implementation.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class TestRocksDbKeyValueStorageTest extends BasicOperationsKeyValueStorageTest {
    private TestRocksDbKeyValueStorage testRocksDbKeyValueStorage;

    @WorkDirectory
    private Path workDir;

    @Override
    protected KeyValueStorage createStorage() {
        testRocksDbKeyValueStorage = new TestRocksDbKeyValueStorage("test", workDir.resolve("storage"));

        return testRocksDbKeyValueStorage;
    }

    @Test
    void testRestoreAfterRestart() {
        byte[] key = key(1);
        byte[] val = keyValue(1, 1);

        Entry e = testRocksDbKeyValueStorage.get(key);

        assertTrue(e.empty());

        putToMs(key, val);

        e = testRocksDbKeyValueStorage.get(key);

        assertArrayEquals(key, e.key());
        assertArrayEquals(val, e.value());

        long revisionBeforeRestart = testRocksDbKeyValueStorage.revision();

        testRocksDbKeyValueStorage.close();

        testRocksDbKeyValueStorage = new TestRocksDbKeyValueStorage("test", workDir.resolve("storage"));

        testRocksDbKeyValueStorage.start();

        assertEquals(revisionBeforeRestart, testRocksDbKeyValueStorage.revision());

        e = testRocksDbKeyValueStorage.get(key);

        assertArrayEquals(key, e.key());
        assertArrayEquals(val, e.value());
    }
}
