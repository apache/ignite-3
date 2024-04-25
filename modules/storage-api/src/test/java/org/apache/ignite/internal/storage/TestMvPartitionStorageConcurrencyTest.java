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

package org.apache.ignite.internal.storage;

import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_PARTITION_COUNT;
import static org.mockito.Mockito.mock;

import org.apache.ignite.internal.storage.impl.TestMvTableStorage;
import org.apache.ignite.internal.storage.impl.TestStorageEngine;
import org.apache.ignite.internal.storage.index.CatalogIndexStatusSupplier;
import org.junit.jupiter.api.BeforeEach;

/**
 * Test implementation for {@link TestStorageEngine}.
 */
public class TestMvPartitionStorageConcurrencyTest extends AbstractMvPartitionStorageConcurrencyTest {
    @BeforeEach
    void setUp() {
        initialize(new TestMvTableStorage(1, DEFAULT_PARTITION_COUNT, mock(CatalogIndexStatusSupplier.class)));
    }
}
