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

import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.impl.TestMvTableStorage;
import org.apache.ignite.internal.storage.index.impl.TestCatalogIndexStatusSupplier;
import org.junit.jupiter.api.BeforeEach;

/**
 * Class for testing the {@link TestMvTableStorage} class.
 */
public class TestMvTableStorageTest extends AbstractMvTableStorageTest {
    @BeforeEach
    void setUp() {
        initialize();
    }

    @Override
    protected MvTableStorage createMvTableStorage() {
        return new TestMvTableStorage(1, DEFAULT_PARTITION_COUNT, new TestCatalogIndexStatusSupplier(catalogService));
    }
}
