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

package org.apache.ignite.internal.cluster.management;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.util.Base64;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.cluster.management.LocalStateStorage.LocalState;
import org.apache.ignite.internal.cluster.management.network.messages.CmgMessagesFactory;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.inmemory.InMemoryVaultService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LocalStateStorageTest {
    private static final CmgMessagesFactory CMG_MESSAGES_FACTORY = new CmgMessagesFactory();

    private VaultManager vault;

    private LocalStateStorage storage;

    @BeforeEach
    void setUp() {
        vault = new VaultManager(new InMemoryVaultService());

        assertThat(vault.startAsync(new ComponentContext()), willCompleteSuccessfully());

        storage = new LocalStateStorage(vault);
    }

    @Test
    void serializationAndDeserialization() {
        LocalState originalState = new LocalState(Set.of("a", "b"), ClusterTag.randomClusterTag(CMG_MESSAGES_FACTORY, "cluster"));

        storage.saveLocalState(originalState);

        LocalState restoredState = storage.getLocalState();

        assertThat(restoredState, is(notNullValue()));
        assertThat(restoredState.cmgNodeNames(), containsInAnyOrder("a", "b"));
        assertThat(restoredState.clusterTag(), is(originalState.clusterTag()));
    }

    @Test
    void v1CanBeDeserialized() {
        vault.put(ByteArray.fromString("cmg_state"), Base64.getDecoder().decode("Ae++QwIBYgFhB2NsdXN0ZXLISSiHAp8QaYmdEJ8HD2mK"));

        LocalState localState = storage.getLocalState();

        assertThat(localState, is(notNullValue()));
        assertThat(localState.cmgNodeNames(), containsInAnyOrder("a", "b"));
        assertThat(localState.clusterTag().clusterName(), is("cluster"));
        assertThat(localState.clusterTag().clusterId(), is(UUID.fromString("69109f02-8728-49c8-8a69-0f079f109d89")));
    }
}
