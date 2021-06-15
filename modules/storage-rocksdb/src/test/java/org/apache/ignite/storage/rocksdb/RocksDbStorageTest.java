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

package org.apache.ignite.storage.rocksdb;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.storage.api.DataRow;
import org.apache.ignite.storage.api.SearchRow;
import org.apache.ignite.storage.api.basic.SimpleDataRow;
import org.apache.ignite.storage.api.basic.SimpleReadInvokeClosure;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class RocksDbStorageTest {
    private Path path;

    private RocksDbStorage storage;

    @BeforeEach
    public void setUp() throws Exception {
        path = Paths.get(/*"workdir", */"rocksdb_test");

        IgniteUtils.delete(path);

        storage = new RocksDbStorage(path, ByteBuffer::compareTo);
    }

    @AfterEach
    public void tearDown() throws Exception {
        try {
            if (storage != null)
                storage.close();
        }
        finally {
            IgniteUtils.delete(path);
        }
    }

    private SearchRow searchRow(String key) {
        return new SimpleDataRow(
            ByteBuffer.wrap(new ByteArray(key).bytes()),
            null
        );
    }

    private DataRow dataRow(String key, String value) {
        return new SimpleDataRow(
            ByteBuffer.wrap(new ByteArray(key).bytes()),
            ByteBuffer.wrap(new ByteArray(value).bytes())
        );
    }

    @Test
    public void readWriteRemove() throws Exception {
        SearchRow searchRow = searchRow("key");

        assertNull(storage.read(searchRow).value());

        DataRow dataRow = dataRow("key", "value");

        storage.write(dataRow);

        assertArrayEquals(dataRow.value().array(), storage.read(searchRow).value().array());

        storage.remove(searchRow);

        assertNull(storage.read(searchRow).value());
    }

    @Test
    void invoke() throws Exception {
        SearchRow searchRow = searchRow("key");

        DataRow dataRow = dataRow("key", "value");

        SimpleReadInvokeClosure readClosure = new SimpleReadInvokeClosure();

        storage.invoke(searchRow, readClosure);

        assertNull(readClosure.row().value());

//        assertArrayEquals(dataRow.value().array(), readClosure.row().value().array());
    }
//
//    @Test
//    void scan() {
//    }
}