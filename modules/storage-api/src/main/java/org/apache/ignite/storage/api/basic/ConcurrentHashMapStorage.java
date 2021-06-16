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

package org.apache.ignite.storage.api.basic;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.storage.api.DataRow;
import org.apache.ignite.storage.api.InvokeClosure;
import org.apache.ignite.storage.api.SearchRow;
import org.apache.ignite.storage.api.Storage;
import org.apache.ignite.storage.api.StorageException;

public class ConcurrentHashMapStorage implements Storage {
    private final ConcurrentMap<ByteArray, byte[]> map = new ConcurrentHashMap<>();

    @Override public DataRow read(SearchRow key) throws StorageException {
        byte[] keyBytes = key.keyBytes();

        byte[] valueBytes = map.get(new ByteArray(keyBytes));

        return new SimpleDataRow(keyBytes, valueBytes);
    }

    @Override public void write(DataRow row) throws StorageException {
        map.put(new ByteArray(row.keyBytes()), row.valueBytes());
    }

    @Override public void remove(SearchRow key) throws StorageException {
        map.remove(new ByteArray(key.keyBytes()));
    }

    @Override public void invoke(SearchRow key, InvokeClosure clo) throws StorageException {
//        map.compute(new ByteArray(key.keyBytes()), (keyBytes, valueBytes) -> {
//            return valueBytes;
//        });
    }

    @Override public Cursor<DataRow> scan(Predicate<SearchRow> filter) throws StorageException {

        return null;
    }
}
