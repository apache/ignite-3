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

import static java.nio.charset.StandardCharsets.UTF_8;

import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

/**
 * Abstract test for {@link KeyValueStorage}.
 */
public abstract class AbstractKeyValueStorageTest extends BaseIgniteAbstractTest {
    protected KeyValueStorage storage;

    @BeforeEach
    public void setUp() {
        storage = createStorage();

        storage.start();
    }

    @AfterEach
    public void tearDown() throws Exception {
        storage.close();
    }

    /**
     * Returns key value storage for this test.
     */
    protected abstract KeyValueStorage createStorage();

    protected static byte[] key(int k) {
        return ("key" + k).getBytes(UTF_8);
    }

    protected static byte[] keyValue(int k, int v) {
        return ("key" + k + '_' + "val" + v).getBytes(UTF_8);
    }
}
