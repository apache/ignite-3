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

package org.apache.ignite.internal.storage.rocksdb;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.rocksdb.ColumnFamily;
import org.apache.ignite.internal.storage.AbstractStorageTest;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

/**
 * Storage test implementation for {@link RocksDbStorage}.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class RocksDbStorageTest extends AbstractStorageTest {
    /** */
    private static final String CF_NAME = "default";

    /** */
    private Options options;

    /** */
    private ColumnFamilyDescriptor cfDescriptor;

    /** */
    private ColumnFamilyHandle cfHandle;

    /** */
    private DBOptions dbOptions;

    /** */
    private RocksDB db;

    /** */
    @BeforeEach
    public void setUp(@WorkDirectory Path workDir) throws RocksDBException {
        options = new Options().setCreateIfMissing(true);

        byte[] cfNameBytes = CF_NAME.getBytes(StandardCharsets.UTF_8);

        cfDescriptor = new ColumnFamilyDescriptor(cfNameBytes, new ColumnFamilyOptions(options));

        List<ColumnFamilyHandle> cfHandles = new ArrayList<>(1);

        dbOptions = new DBOptions().setCreateIfMissing(true);

        db = RocksDB.open(dbOptions, workDir.toString(), List.of(cfDescriptor), cfHandles);

        cfHandle = cfHandles.get(0);

        ColumnFamily cf = new ColumnFamily(db, cfHandle, CF_NAME, cfDescriptor.getOptions(), this.options);

        storage = new RocksDbStorage(db, cf);
    }

    /** */
    @AfterEach
    public void tearDown() throws Exception {
        IgniteUtils.closeAll(storage, cfHandle, db, dbOptions, cfDescriptor.getOptions(), options);
    }
}
