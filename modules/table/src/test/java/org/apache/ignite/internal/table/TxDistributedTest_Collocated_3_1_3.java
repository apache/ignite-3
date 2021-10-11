/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.table;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import static org.junit.jupiter.api.Assertions.assertSame;

/** Tests a tx suite on a cluster node collocated to accounts leader. */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class TxDistributedTest_Collocated_3_1_3 extends TxDistributedTest_3_1_3 {
    /**
     * @param testInfo Test info.
     */
    public TxDistributedTest_Collocated_3_1_3(TestInfo testInfo) {
        super(testInfo);
    }

    /** {@inheritDoc} */
    @Override protected boolean startClient() {
        return false;
    }

    /** {@inheritDoc} */
    @BeforeEach
    @Override public void before() throws Exception {
        super.before();

        assertSame(accRaftClients.get(0).clusterService(), getLeader(accRaftClients.get(0)).clusterService());
    }
}
