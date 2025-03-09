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

package org.apache.ignite.internal.network;

import java.util.Collection;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.network.NetworkAddress;

/**
 * Interface for services responsible for discovering the initial set of network cluster members.
 */
public interface NodeFinder extends ManuallyCloseable {
    /**
     * Discovers the initial set of cluster members and returns their network addresses.
     *
     * @return addresses of initial cluster members.
     */
    Collection<NetworkAddress> findNodes();

    @Override
    default void close() {
        // No-op.
    }

    /** Starts the node finder. */
    void start();
}
