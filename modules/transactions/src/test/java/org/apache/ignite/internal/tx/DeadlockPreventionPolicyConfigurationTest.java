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

package org.apache.ignite.internal.tx;

import static org.apache.ignite.internal.replicator.ReplicaService.DEFAULT_REPLICA_OPERATION_RETRY_INTERVAL;
import static org.apache.ignite.internal.tx.impl.DeadlockPreventionPolicyImpl.TxIdComparators.NATURAL;
import static org.apache.ignite.internal.tx.impl.DeadlockPreventionPolicyImpl.TxIdComparators.NONE;
import static org.apache.ignite.internal.tx.impl.DeadlockPreventionPolicyImpl.TxIdComparators.REVERSE;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Comparator;
import java.util.UUID;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.tx.configuration.TransactionConfiguration;
import org.apache.ignite.internal.tx.impl.DeadlockPreventionPolicyImpl;
import org.apache.ignite.internal.tx.impl.DeadlockPreventionPolicyImpl.TxIdComparators;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for {@link org.apache.ignite.internal.tx.configuration.DeadlockPreventionPolicyConfigurationSchema}.
 */
@ExtendWith(ConfigurationExtension.class)
public class DeadlockPreventionPolicyConfigurationTest {
    @InjectConfiguration
    private TransactionConfiguration transactionConfigurationDefault;

    @InjectConfiguration("mock: { deadlockPreventionPolicy: { waitTimeout: 1000, txIdComparator: NATURAL } }")
    private TransactionConfiguration transactionConfigurationMockedNatural;

    @InjectConfiguration("mock: { deadlockPreventionPolicy: { waitTimeout: 100, txIdComparator: REVERSE } }")
    private TransactionConfiguration transactionConfigurationMockedReverse;

    @InjectConfiguration("mock: { deadlockPreventionPolicy: { waitTimeout: 0, txIdComparator: NONE } }")
    private TransactionConfiguration transactionConfigurationMockedNone;

    @InjectConfiguration("mock: { replicaOperationRetryInterval: 1000 }")
    private TransactionConfiguration transactionConfigurationMockedInterval;

    @Test
    public void checkDefaults() {
        assertEquals(0, transactionConfigurationDefault.deadlockPreventionPolicy().waitTimeout().value());
        assertEquals(NATURAL.toString(),
                transactionConfigurationDefault.deadlockPreventionPolicy().txIdComparator().value());
        assertEquals(DEFAULT_REPLICA_OPERATION_RETRY_INTERVAL, transactionConfigurationDefault.replicaOperationRetryInterval().value());

        assertPolicyIsCorrect(createPolicy(transactionConfigurationDefault), transactionConfigurationDefault);
    }

    @Test
    public void checkMockedInterval() {
        assertEquals(1000, transactionConfigurationMockedInterval.replicaOperationRetryInterval().value());
    }

    @Test
    public void checkMockedNatural() {
        assertEquals(1000, transactionConfigurationMockedNatural.deadlockPreventionPolicy().waitTimeout().value());
        assertEquals(NATURAL.toString(),
                transactionConfigurationMockedNatural.deadlockPreventionPolicy().txIdComparator().value());

        assertPolicyIsCorrect(createPolicy(transactionConfigurationMockedNatural), transactionConfigurationMockedNatural);
    }

    @Test
    public void checkMockedReverse() {
        assertEquals(100, transactionConfigurationMockedReverse.deadlockPreventionPolicy().waitTimeout().value());
        assertEquals(REVERSE.toString(),
                transactionConfigurationMockedReverse.deadlockPreventionPolicy().txIdComparator().value());

        assertPolicyIsCorrect(createPolicy(transactionConfigurationMockedReverse), transactionConfigurationMockedReverse);
    }

    @Test
    public void checkMockedNone() {
        assertEquals(0, transactionConfigurationMockedNone.deadlockPreventionPolicy().waitTimeout().value());
        assertEquals(NONE.toString(),
                transactionConfigurationMockedNone.deadlockPreventionPolicy().txIdComparator().value());

        assertPolicyIsCorrect(createPolicy(transactionConfigurationMockedNone), transactionConfigurationMockedNone);
    }

    private void assertPolicyIsCorrect(DeadlockPreventionPolicy policy, TransactionConfiguration cfg) {
        assertEquals(cfg.deadlockPreventionPolicy().waitTimeout().value(), policy.waitTimeout());
        assertComparatorIsCorrect(policy.txIdComparator(), cfg.deadlockPreventionPolicy().txIdComparator().value());
    }

    private DeadlockPreventionPolicy createPolicy(TransactionConfiguration cfg) {
        return new DeadlockPreventionPolicyImpl(
                cfg.deadlockPreventionPolicy().txIdComparator().value(),
                cfg.deadlockPreventionPolicy().waitTimeout().value()
        );
    }

    private void assertComparatorIsCorrect(@Nullable Comparator<UUID> actualComparator, String configured) {
        TxIdComparators configuredEnum = TxIdComparators.valueOf(configured);

        if (actualComparator == null) {
            assertEquals(NONE, configuredEnum);
            return;
        }

        UUID uuid1 = UUID.randomUUID();
        UUID uuid2;

        do {
            uuid2 = UUID.randomUUID();
        } while (uuid1.compareTo(uuid2) == 0);

        UUID greater = uuid1.compareTo(uuid2) > 0 ? uuid1 : uuid2;
        UUID lesser = uuid1.compareTo(uuid2) > 0 ? uuid2 : uuid1;

        int cmp = actualComparator.compare(greater, lesser);
        if (cmp > 0) {
            assertEquals(NATURAL, configuredEnum);
        } else {
            assertEquals(REVERSE, configuredEnum);
        }
    }
}
