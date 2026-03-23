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

package org.apache.ignite.internal.app;

import static org.apache.ignite.internal.app.NodePropertiesImpl.ZONE_BASED_REPLICATION_KEY;
import static org.apache.ignite.internal.lang.IgniteSystemProperties.COLOCATION_FEATURE_FLAG;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.vault.VaultEntry;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.IgniteException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class NodePropertiesImplTest extends BaseIgniteAbstractTest {
    @Mock
    private VaultManager vaultManager;

    private NodePropertiesImpl nodeProperties;

    @BeforeEach
    void init() {
        nodeProperties = new NodePropertiesImpl(vaultManager);
    }

    @Test
    void throwsWhenUsedBeforeStart() {
        assertThrows(IllegalStateException.class, () -> nodeProperties.colocationEnabled());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void freshNodeTakesStatusFromSystemProperty(boolean enableColocation) {
        doReturn(null).when(vaultManager).get(ZONE_BASED_REPLICATION_KEY);
        doReturn(null).when(vaultManager).name();

        runWithColocationProperty(String.valueOf(enableColocation), () -> {
            if (!enableColocation) {
                assertThrowsWithCause(
                        () -> startComponent(),
                        IgniteException.class,
                        "Table based replication is no longer supported, consider restarting the node in zone based replication mode.");
            } else {
                startComponent();

                assertThat(nodeProperties.colocationEnabled(), is(enableColocation));

                verifyStatusSavedToVault(enableColocation);
            }
        });
    }

    private static void runWithColocationProperty(String propertyValue, Runnable action) {
        String oldValue = System.getProperty(COLOCATION_FEATURE_FLAG);
        System.setProperty(COLOCATION_FEATURE_FLAG, propertyValue);

        try {
            action.run();
        } finally {
            if (oldValue == null) {
                System.clearProperty(COLOCATION_FEATURE_FLAG);
            } else {
                System.setProperty(COLOCATION_FEATURE_FLAG, oldValue);
            }
        }
    }

    private void startComponent() {
        assertThat(nodeProperties.startAsync(new ComponentContext()), willCompleteSuccessfully());
    }

    private void verifyStatusSavedToVault(boolean enableColocation) {
        verify(vaultManager).put(ZONE_BASED_REPLICATION_KEY, statusRepresentationAsBytes(enableColocation));
    }

    private static byte[] statusRepresentationAsBytes(boolean enableColocation) {
        return new byte[]{(byte) (enableColocation ? 1 : 0)};
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void usedNodeTakesStatusFromVaultIfSaved(boolean originalColocationStatus) {
        doReturn(new VaultEntry(ZONE_BASED_REPLICATION_KEY, statusRepresentationAsBytes(originalColocationStatus)))
                .when(vaultManager).get(ZONE_BASED_REPLICATION_KEY);
        lenient().doReturn("test").when(vaultManager).name();

        boolean oppositeColocationStatus = !originalColocationStatus;
        runWithColocationProperty(String.valueOf(oppositeColocationStatus), () -> {
            if (!originalColocationStatus) {
                assertThrowsWithCause(
                        () -> startComponent(),
                        IgniteException.class,
                        "Table based replication is no longer supported."
                                + " Downgrade back to 3.1 and copy your data to a cluster of desired version.");
            } else {
                startComponent();

                assertThat(nodeProperties.colocationEnabled(), is(originalColocationStatus));

                verifyStatusNotWrittenToVault();
            }
        });
    }

    private void verifyStatusNotWrittenToVault() {
        verify(vaultManager, never()).put(eq(ZONE_BASED_REPLICATION_KEY), any());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void usedNodeWithoutStatusInVaultConsidersColocationDisabled(boolean colocationStatusFromSystemProps) {
        doReturn(null).when(vaultManager).get(ZONE_BASED_REPLICATION_KEY);
        doReturn("test").when(vaultManager).name();

        runWithColocationProperty(String.valueOf(colocationStatusFromSystemProps), () -> {
            assertThrowsWithCause(
                    () -> startComponent(),
                    IgniteException.class,
                    "Table based replication is no longer supported."
                            + " Downgrade back to 3.1 and copy your data to a cluster of desired version.");
        });
    }

    @Test
    void stopsSuccessfully() {
        startComponent();

        assertThat(nodeProperties.stopAsync(new ComponentContext()), willCompleteSuccessfully());
    }
}
