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

package org.apache.ignite.internal.network.recovery.message;

import static org.apache.ignite.internal.network.recovery.message.HandshakeRejectionReason.CLINCH;
import static org.apache.ignite.internal.network.recovery.message.HandshakeRejectionReason.CLUSTER_ID_MISMATCH;
import static org.apache.ignite.internal.network.recovery.message.HandshakeRejectionReason.LOOP;
import static org.apache.ignite.internal.network.recovery.message.HandshakeRejectionReason.PRODUCT_MISMATCH;
import static org.apache.ignite.internal.network.recovery.message.HandshakeRejectionReason.STALE_LAUNCH_ID;
import static org.apache.ignite.internal.network.recovery.message.HandshakeRejectionReason.STOPPING;
import static org.apache.ignite.internal.network.recovery.message.HandshakeRejectionReason.VERSION_MISMATCH;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.EnumSet;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.junit.jupiter.api.Test;

class HandshakeRejectionReasonTest {
    @Test
    void logAsWarn() {
        var assertions = new EnumMembersAssertions();

        assertions.assertFalseFor(STOPPING, HandshakeRejectionReason::logAsWarn);
        assertions.assertTrueFor(LOOP, HandshakeRejectionReason::logAsWarn);
        assertions.assertTrueFor(STALE_LAUNCH_ID, HandshakeRejectionReason::logAsWarn);
        assertions.assertFalseFor(CLINCH, HandshakeRejectionReason::logAsWarn);
        assertions.assertTrueFor(CLUSTER_ID_MISMATCH, HandshakeRejectionReason::logAsWarn);
        assertions.assertTrueFor(PRODUCT_MISMATCH, HandshakeRejectionReason::logAsWarn);
        assertions.assertTrueFor(VERSION_MISMATCH, HandshakeRejectionReason::logAsWarn);

        assertions.assertAllAsserted();
    }

    private static class EnumMembersAssertions {
        private final Set<HandshakeRejectionReason> assertedReasons = EnumSet.noneOf(HandshakeRejectionReason.class);

        private void assertTrueFor(HandshakeRejectionReason reason, Predicate<HandshakeRejectionReason> predicate) {
            addAndAssert(reason, toAssert -> assertTrue(predicate.test(toAssert)));
        }

        private void assertFalseFor(HandshakeRejectionReason reason, Predicate<HandshakeRejectionReason> predicate) {
            addAndAssert(reason, toAssert -> assertFalse(predicate.test(toAssert)));
        }

        private void addAndAssert(HandshakeRejectionReason reason, Consumer<HandshakeRejectionReason> assertion) {
            assertion.accept(reason);
            assertedReasons.add(reason);
        }

        void assertAllAsserted() {
            assertThat(assertedReasons, containsInAnyOrder(HandshakeRejectionReason.values()));
        }
    }
}
