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
import static java.util.stream.Collectors.joining;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.UUID;
import java.util.stream.IntStream;
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
        when(vaultManager.get(staleIdsKey)).thenReturn(new VaultEntry(staleIdsKey, staleIdsBytes(1, 2, 3)));

        assertThat(this.staleIds.isIdStale(id(1)), is(true));
        assertThat(this.staleIds.isIdStale(id(2)), is(true));
        assertThat(this.staleIds.isIdStale(id(3)), is(true));
        assertThat(this.staleIds.isIdStale(id(10)), is(false));
    }

    private static UUID id(int id) {
        return new UUID(0, id);
    }

    private static byte[] staleIdsBytes(int... ids) {
        String staleIds = IntStream.of(ids)
                .mapToObj(VaultStaleIdsTest::id)
                .map(UUID::toString)
                .collect(joining("\n"));
        return staleIds.getBytes(UTF_8);
    }

    @Test
    void cachesVaultStateInMemory() {
        when(vaultManager.get(staleIdsKey)).thenReturn(new VaultEntry(staleIdsKey, staleIdsBytes(1, 2, 3)));

        staleIds.isIdStale(id(1));
        staleIds.isIdStale(id(2));
        staleIds.isIdStale(id(3));

        verify(vaultManager).get(any());
    }

    @Test
    void savesNewStaleIdsToVault() {
        staleIds.markAsStale(id(2));

        verify(vaultManager).put(staleIdsKey, staleIdsBytes(2));

        staleIds.markAsStale(id(1));

        verify(vaultManager).put(staleIdsKey, staleIdsBytes(2, 1));
    }

    @Test
    void respectsMaxIdsLimit() {
        staleIds = new VaultStaleIds(vaultManager, 2);

        staleIds.markAsStale(id(3));
        staleIds.markAsStale(id(2));
        staleIds.markAsStale(id(1));

        ArgumentCaptor<byte[]> idsCaptor = ArgumentCaptor.forClass(byte[].class);

        verify(vaultManager, times(3)).put(eq(staleIdsKey), idsCaptor.capture());

        assertThat(idsCaptor.getValue(), is(staleIdsBytes(2, 1)));
    }

    @Test
    void loadsBeforeDoingFirstSave() {
        when(vaultManager.get(staleIdsKey)).thenReturn(new VaultEntry(staleIdsKey, staleIdsBytes(1)));

        staleIds.markAsStale(id(2));

        verify(vaultManager).put(staleIdsKey, staleIdsBytes(1, 2));
    }
}
