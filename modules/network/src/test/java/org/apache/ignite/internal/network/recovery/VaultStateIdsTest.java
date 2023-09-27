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

package org.apache.ignite.internal.network.recovery;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.vault.VaultEntry;
import org.apache.ignite.internal.vault.VaultManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class VaultStateIdsTest extends BaseIgniteAbstractTest {
    @Mock
    private VaultManager vaultManager;

    private final ByteArray staleIdsKey = new ByteArray("network.staleIds");

    private VaultStateIds staleIds;

    @BeforeEach
    void createObjectToTest() {
        staleIds = new VaultStateIds(vaultManager);
    }

    @Test
    void consultsVaultWhenCheckingForStaleness() {
        doReturn(completedFuture(new VaultEntry(staleIdsKey, "id1\nid2\nid3".getBytes(UTF_8))))
                .when(vaultManager).get(staleIdsKey);

        assertThat(staleIds.isIdStale("id1"), is(true));
        assertThat(staleIds.isIdStale("id2"), is(true));
        assertThat(staleIds.isIdStale("id3"), is(true));
        assertThat(staleIds.isIdStale("id10"), is(false));
    }

    @Test
    void cachesVaultStateInMemory() {
        doReturn(completedFuture(new VaultEntry(staleIdsKey, "id1\nid2\nid3".getBytes(UTF_8))))
                .when(vaultManager).get(staleIdsKey);

        staleIds.isIdStale("id1");
        staleIds.isIdStale("id2");
        staleIds.isIdStale("id3");

        verify(vaultManager, times(1)).get(any());
    }

    @Test
    void savesNewStaleIdsToVault() {
        doReturn(completedFuture(null)).when(vaultManager).get(staleIdsKey);
        doReturn(completedFuture(null))
                .when(vaultManager).put(staleIdsKey, "id2".getBytes(UTF_8));
        doReturn(completedFuture(null))
                .when(vaultManager).put(staleIdsKey, "id2\nid1".getBytes(UTF_8));

        staleIds.markAsStale("id2");
        staleIds.markAsStale("id1");
    }

    @Test
    void respectsMaxIdsLimit() {
        staleIds = new VaultStateIds(vaultManager, 2);

        doReturn(completedFuture(null)).when(vaultManager).get(staleIdsKey);

        AtomicReference<String> lastSavedIds = new AtomicReference<>();

        doAnswer(invocation -> {
            byte[] value = invocation.getArgument(1);

            lastSavedIds.set(new String(value, UTF_8));

            return completedFuture(null);
        }).when(vaultManager).put(eq(staleIdsKey), any());

        staleIds.markAsStale("id3");
        staleIds.markAsStale("id2");
        staleIds.markAsStale("id1");

        assertThat(lastSavedIds.get(), is("id2\nid1"));
    }

    @Test
    void loadsBeforeDoingFirstSave() {
        lenient().doReturn(completedFuture(new VaultEntry(staleIdsKey, "id1".getBytes(UTF_8))))
                .when(vaultManager).get(staleIdsKey);
        doReturn(completedFuture(null)).when(vaultManager).put(eq(staleIdsKey), any());

        staleIds.markAsStale("id2");

        verify(vaultManager).put(staleIdsKey, "id1\nid2".getBytes(UTF_8));
    }
}
