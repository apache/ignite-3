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

package org.apache.ignite.internal.tx.test;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Objects;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.TxStateMeta;

/**
 * Test utils for {@link TxStateMeta}.
 */
public class TxStateMetaTestUtils {
    private static final IgniteLogger log = Loggers.forClass(TxStateMetaTestUtils.class);

    /**
     * Asserts that two TxStateMeta are the same.
     *
     * @param meta0 TxStateMeta.
     * @param meta1 TxStateMeta.
     */
    public static void assertTxStateMetaIsSame(TxStateMeta meta0, TxStateMeta meta1) {
        boolean res = meta0.txState() == meta1.txState()
                && Objects.equals(meta0.txCoordinatorId(), meta1.txCoordinatorId())
                && Objects.equals(meta0.commitPartitionId(), meta1.commitPartitionId())
                && (meta0.txState() == TxState.FINISHING || Objects.equals(meta0.commitTimestamp(), meta1.commitTimestamp()))
                && Objects.equals(meta0.txLabel(), meta1.txLabel());

        if (!res) {
            log.error("TxStateMeta are not the same:\nmeta0: {}\nmeta1: {}", meta0, meta1);
        }

        assertTrue(res);
    }
}
