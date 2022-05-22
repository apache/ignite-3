/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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

package org.apache.ignite.internal.storage.basic;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.internal.storage.AbstractSortedIndexMvStorageTest;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.index.SortedIndexMvStorage;

/**
 * MV sorted index storage test implementation for {@link TestSortedIndexMvStorage} class.
 */
public class TestSortedIndexMvStorageTest extends AbstractSortedIndexMvStorageTest {
    private List<TestSortedIndexMvStorage> indexes = new CopyOnWriteArrayList<>();

    private TestMvPartitionStorage partitionStorage = new TestMvPartitionStorage(indexes, 0);

    @Override
    protected MvPartitionStorage partitionStorage() {
        return partitionStorage;
    }

    @Override
    protected SortedIndexMvStorage createIndexStorage(String name, TableView tableCfg) {
        TestSortedIndexMvStorage index = new TestSortedIndexMvStorage(name, tableCfg, schemaDescriptor, Map.of(0, partitionStorage));

        indexes.add(index);

        return index;
    }
}
