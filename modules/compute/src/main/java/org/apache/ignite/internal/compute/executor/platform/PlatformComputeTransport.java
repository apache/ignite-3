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

package org.apache.ignite.internal.compute.executor.platform;

import java.util.concurrent.CompletableFuture;

/**
 * Interface for the transport layer of the platform compute executor.
 *
 * <p>This interface is responsible for managing connections to external executors
 * and providing the necessary methods to send and receive messages.
 */
public interface PlatformComputeTransport {
    /**
     * Gets the server address for the external executor to connect to.
     *
     * @return Server address.
     */
    String serverAddress();

    /**
     * Gets a value indicating whether SSL is enabled on the transport.
     *
     * @return True if SSL is enabled; otherwise, false.
     */
    boolean sslEnabled();

    /**
     * Registers a single-use secret for a platform compute connection.
     *
     * @param computeExecutorId Compute executor id.
     * @return Future that will complete once the connection with the specified executor id is established.
     */
    CompletableFuture<PlatformComputeConnection> registerComputeExecutorId(String computeExecutorId);
}
