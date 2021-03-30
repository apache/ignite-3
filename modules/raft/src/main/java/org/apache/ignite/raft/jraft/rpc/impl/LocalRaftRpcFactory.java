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
package org.apache.ignite.raft.jraft.rpc.impl;

import org.apache.ignite.raft.jraft.option.RpcOptions;
import org.apache.ignite.raft.jraft.rpc.RaftRpcFactory;
import org.apache.ignite.raft.jraft.rpc.RpcClient;
import org.apache.ignite.raft.jraft.rpc.RpcServer;
import org.apache.ignite.raft.jraft.util.Endpoint;
import org.apache.ignite.raft.jraft.util.SPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author jiachun.fjc
 */
@SPI
public class LocalRaftRpcFactory implements RaftRpcFactory {
    private static final Logger LOG                               = LoggerFactory.getLogger(LocalRaftRpcFactory.class);
    @Override public RpcClient createRpcClient(ConfigHelper<RpcClient> helper) {
        LocalRpcClient rpcClient = new LocalRpcClient();

        if (helper != null)
            helper.config(rpcClient);

        return rpcClient;
    }

    @Override public RpcServer createRpcServer(Endpoint endpoint, ConfigHelper<RpcServer> helper) {
        LocalRpcServer srv = new LocalRpcServer(endpoint);

        if (helper != null)
            helper.config(srv);

        return srv;
    }

    @Override public ConfigHelper<RpcServer> defaultJRaftServerConfigHelper(RpcOptions opts) {
        return new ConfigHelper<RpcServer>() {
            @Override public void config(RpcServer instance) {
                LocalRpcServer srv = (LocalRpcServer) instance;
                // TODO asch.
            }
        };
    }

    @Override
    public ConfigHelper<RpcClient> defaultJRaftClientConfigHelper(final RpcOptions opts) {
        return new ConfigHelper<RpcClient>() {
            @Override public void config(RpcClient instance) {
                LocalRpcClient rpcClient = (LocalRpcClient) instance;
                // TODO asch.
            }
        };
    }
}
