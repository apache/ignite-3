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

package org.apache.ignite.internal.replicator.listener;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.replicator.message.ReplicaRequestLocator;

/**
 * The listener response returns when the replication process has some intermediate result, and also it will continue in the future.
 */
public class ListenerCompoundResponse implements ListenerResponse {
    /**
     * Operation locator.
     */
    private final ReplicaRequestLocator locator;

    /**
     * Result future.
     */
    private final CompletableFuture resultFut;

    /**
     * Result.
     */
    private Object result;

    /**
     * The constructor.
     *
     * @param locator Operation locator.
     * @param resultFut Result future.
     * @param result Instant result.
     */
    public ListenerCompoundResponse(ReplicaRequestLocator locator, CompletableFuture resultFut, Object result) {
        this.locator = locator;
        this.resultFut = resultFut;
        this.result = result;
    }

    /**
     * Gets a result.
     *
     * @return Some result of the replication process.
     */
    public Object result() {
        return result;
    }

    /**
     * Get an operation locator.
     *
     * @return Operation locator.
     */
    public ReplicaRequestLocator operationLocator() {
        return locator;
    }

    /**
     * Result future.
     *
     * @return Future.
     */
    public CompletableFuture resultFuture() {
        return resultFut;
    }
}
