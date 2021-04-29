/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.raft.jraft.rpc;

import org.apache.ignite.raft.jraft.rpc.impl.LocalRpcClient;
import org.apache.ignite.raft.jraft.rpc.impl.LocalRpcServer;
import org.apache.ignite.raft.jraft.util.Endpoint;
import org.junit.Ignore;

/**
 * TODO asch local rpc can be removed.
 */
@Ignore
public class LocalRpcTest extends AbstractRpcTest {
    /** {@inheritDoc} */
    @Override public RpcServer createServer(Endpoint endpoint) {
        return new LocalRpcServer(endpoint);
    }

    /** {@inheritDoc} */
    @Override public RpcClient createClient() {
        return new LocalRpcClient();
    }

    /** {@inheritDoc} */
    @Override protected boolean waitForTopology(RpcClient client, int expected, long timeout) {
        return true;
    }
}
