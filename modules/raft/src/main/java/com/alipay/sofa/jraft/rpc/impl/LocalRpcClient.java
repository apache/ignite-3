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
package com.alipay.sofa.jraft.rpc.impl;

import com.alipay.sofa.jraft.ReplicatorGroup;
import com.alipay.sofa.jraft.error.RemotingException;
import com.alipay.sofa.jraft.option.RpcOptions;
import com.alipay.sofa.jraft.rpc.InvokeCallback;
import com.alipay.sofa.jraft.rpc.InvokeContext;
import com.alipay.sofa.jraft.rpc.RpcClient;
import com.alipay.sofa.jraft.util.Endpoint;

/**
 * Bolt rpc client impl.
 *
 * @author jiachun.fjc
 */
public class LocalRpcClient implements RpcClient {
    @Override public boolean checkConnection(Endpoint endpoint) {
        return false;
    }

    @Override public boolean checkConnection(Endpoint endpoint, boolean createIfAbsent) {
        return false;
    }

    @Override public void closeConnection(Endpoint endpoint) {

    }

    @Override public void registerConnectEventListener(ReplicatorGroup replicatorGroup) {

    }

    @Override public Object invokeSync(Endpoint endpoint, Object request, InvokeContext ctx, long timeoutMs) throws InterruptedException, RemotingException {
        return null;
    }

    @Override public void invokeAsync(Endpoint endpoint, Object request, InvokeContext ctx, InvokeCallback callback, long timeoutMs) throws InterruptedException, RemotingException {

    }

    @Override public boolean init(RpcOptions opts) {
        return false;
    }

    @Override public void shutdown() {

    }
}
