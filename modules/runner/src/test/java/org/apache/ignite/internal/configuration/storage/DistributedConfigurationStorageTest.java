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

package org.apache.ignite.internal.configuration.storage;

import static org.apache.ignite.internal.testframework.flow.TestFlowUtils.fromCursor;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.metastorage.dsl.SimpleCondition;
import org.apache.ignite.internal.metastorage.server.Condition;
import org.apache.ignite.internal.metastorage.server.ExistenceCondition;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.RevisionCondition;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

/**
 * Tests for the {@link DistributedConfigurationStorage}.
 */
public class DistributedConfigurationStorageTest extends ConfigurationStorageTest {
    private final KeyValueStorage metaStorage = new SimpleInMemoryKeyValueStorage("test");

    private final MetaStorageManager metaStorageManager = mockMetaStorageManager();

    /**
     * Before each.
     */
    @BeforeEach
    void start() {
        metaStorage.start();
        metaStorageManager.startAsync();
    }

    /**
     * After each.
     */
    @AfterEach
    void stop() throws Exception {
        metaStorageManager.stopAsync();
        metaStorage.close();
    }

    /** {@inheritDoc} */
    @Override
    public ConfigurationStorage getStorage() {
        return new DistributedConfigurationStorage("test", metaStorageManager);
    }

    /**
     * Creates a mock implementation of a {@link MetaStorageManager}.
     */
    private MetaStorageManager mockMetaStorageManager() {
        var mock = mock(MetaStorageManager.class);

        when(mock.invoke(any(), anyCollection(), anyCollection())).thenAnswer(invocation -> {
            SimpleCondition condition = invocation.getArgument(0);
            Collection<Operation> success = invocation.getArgument(1);
            Collection<Operation> failure = invocation.getArgument(2);

            boolean invokeResult = metaStorage.invoke(toServerCondition(condition), success, failure, HybridTimestamp.MIN_VALUE);

            return CompletableFuture.completedFuture(invokeResult);
        });

        when(mock.prefix(any())).thenAnswer(invocation -> {
            ByteArray prefix = invocation.getArgument(0);

            return fromCursor(metaStorage.range(prefix.bytes(), metaStorage.nextKey(prefix.bytes())));
        });

        return mock;
    }

    /**
     * Converts a {@link SimpleCondition} to a {@link Condition}.
     */
    private static Condition toServerCondition(SimpleCondition condition) {
        switch (condition.type()) {
            case REV_LESS_OR_EQUAL:
                return new RevisionCondition(
                        RevisionCondition.Type.LESS_OR_EQUAL,
                        condition.key(),
                        ((SimpleCondition.RevisionCondition) condition).revision()
                );
            case KEY_NOT_EXISTS:
                return new ExistenceCondition(
                        ExistenceCondition.Type.NOT_EXISTS,
                        condition.key()
                );
            default:
                throw new UnsupportedOperationException("Unsupported condition type: " + condition.type());
        }
    }
}
