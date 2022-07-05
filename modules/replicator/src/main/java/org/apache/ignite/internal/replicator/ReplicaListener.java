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

package org.apache.ignite.internal.replicator;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.replicator.message.ReplicaResponse;
import org.apache.ignite.lang.IgniteBiTuple;

/**
 * Replica listener.
 * TODO:IGNITE-17258 Implement ReplicaListener.
 */
public interface ReplicaListener {

    /**
     * Invokes Replica listener to process request.
     * If the request does not trigger anything in the future, the second attribute in a tuple result is {@code null}.
     *
     * @param request Replica request.
     * @return Tuple of an instant response and a future to further asynchronous process.
     */
    public IgniteBiTuple<ReplicaResponse, CompletableFuture> invoke(ReplicaRequest request);
}
