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
package org.apache.ignite.raft.rpc;

import java.util.concurrent.Executor;

/**
 *
 */
public interface RpcClient {
    /**
     * Check connection for given address.
     *
     * @param node target address
     * @return true if there is a connection and the connection is active and writable.
     */
    boolean checkConnection(final Node node);

    /**
     * Check connection for given address and async to create a new one if there is no connection.
     *
     * @param node       target address
     * @param createIfAbsent create a new one if there is no connection
     * @return true if there is a connection and the connection is active and writable.
     */
    boolean checkConnection(final Node node, final boolean createIfAbsent);

    /**
     * Close all connections of a address.
     *
     * @param node target address
     */
    void closeConnection(final Node node);

    /**
     * Asynchronous invocation with a callback.
     *
     * @param node  Target node.
     * @param request   Request object
     * @param callback  Invoke callback.
     * @param executor  Executor to run invoke callback.
     * @param timeoutMs Timeout millisecond.
     */
    void invokeAsync(final Node node, final Message request, InvokeCallback callback, Executor executor, final long timeoutMs);
}
