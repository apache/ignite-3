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

package org.apache.ignite.distributed;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

import static org.junit.jupiter.api.Assertions.assertSame;

/** Tests a tx suite on a cluster node collocated to accounts and customers leader. */
public class ItTxDistributedTest_Collocated_1_1_1 extends ItTxDistributedTest_1_1_1 {
    /**
     * @param testInfo Test info.
     */
    public ItTxDistributedTest_Collocated_1_1_1(TestInfo testInfo) {
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

        assertSame(accRaftClients.get(0).clusterService(), getLeader(accRaftClients.get(0)).service());
        assertSame(custRaftClients.get(0).clusterService(), getLeader(custRaftClients.get(0)).service());
    }
}



