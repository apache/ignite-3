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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.vault.VaultEntry;
import org.apache.ignite.internal.vault.VaultManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class VaultStaleIdsTest extends BaseIgniteAbstractTest {
    @Mock
    private VaultManager vaultManager;

    private final ByteArray staleIdsKey = new ByteArray("network.staleIds");

    private VaultStaleIds staleIds;

    @BeforeEach
    void createObjectToTest() {
        staleIds = new VaultStaleIds(vaultManager);
    }

    @Test
    void consultsVaultWhenCheckingForStaleness() {
        when(vaultManager.get(staleIdsKey)).thenReturn(new VaultEntry(staleIdsKey, "id1\nid2\nid3".getBytes(UTF_8)));

        assertThat(staleIds.isIdStale("id1"), is(true));
        assertThat(staleIds.isIdStale("id2"), is(true));
        assertThat(staleIds.isIdStale("id3"), is(true));
        assertThat(staleIds.isIdStale("id10"), is(false));
    }

    @Test
    void cachesVaultStateInMemory() {
        when(vaultManager.get(staleIdsKey)).thenReturn(new VaultEntry(staleIdsKey, "id1\nid2\nid3".getBytes(UTF_8)));

        staleIds.isIdStale("id1");
        staleIds.isIdStale("id2");
        staleIds.isIdStale("id3");

        verify(vaultManager).get(any());
    }

    @Test
    void savesNewStaleIdsToVault() {
        staleIds.markAsStale("id2");

        verify(vaultManager).put(staleIdsKey, "id2".getBytes(UTF_8));

        staleIds.markAsStale("id1");

        verify(vaultManager).put(staleIdsKey, "id2\nid1".getBytes(UTF_8));
    }

    @Test
    void respectsMaxIdsLimit() {
        staleIds = new VaultStaleIds(vaultManager, 2);

        staleIds.markAsStale("id3");
        staleIds.markAsStale("id2");
        staleIds.markAsStale("id1");

        ArgumentCaptor<byte[]> idsCaptor = ArgumentCaptor.forClass(byte[].class);

        verify(vaultManager, times(3)).put(eq(staleIdsKey), idsCaptor.capture());

        assertThat(idsCaptor.getValue(), is("id2\nid1".getBytes(UTF_8)));
    }

    @Test
    void loadsBeforeDoingFirstSave() {
        when(vaultManager.get(staleIdsKey)).thenReturn(new VaultEntry(staleIdsKey, "id1".getBytes(UTF_8)));

        staleIds.markAsStale("id2");

        verify(vaultManager).put(staleIdsKey, "id1\nid2".getBytes(UTF_8));
    }
}
