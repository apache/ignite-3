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

package org.apache.ignite.raft.jraft.test;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;

import org.apache.ignite.raft.jraft.entity.PeerId;
import org.junit.jupiter.api.TestInfo;

public class TestPeer {
    private final PeerId peerId;

    private final int port;

    public TestPeer(TestInfo testInfo, int port, int priority) {
        this.peerId = new PeerId(testNodeName(testInfo, port), 0, priority);
        this.port = port;
    }

    public TestPeer(TestInfo testInfo, int port) {
        this.peerId = new PeerId(testNodeName(testInfo, port));
        this.port = port;
    }

    public PeerId getPeerId() {
        return peerId;
    }

    public int getPort() {
        return port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TestPeer testPeer = (TestPeer) o;

        if (port != testPeer.port) {
            return false;
        }
        return peerId.equals(testPeer.peerId);
    }

    @Override
    public int hashCode() {
        int result = peerId.hashCode();
        result = 31 * result + port;
        return result;
    }
}
