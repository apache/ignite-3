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

package org.apache.ignite.raft.client.message;

import java.util.List;
import org.apache.ignite.network.message.NetworkMessage;
import org.apache.ignite.raft.client.Peer;

/**
 *
 */
public interface GetPeersResponse extends NetworkMessage {
    /**
     * @return Current peers.
     */
    List<Peer> peers();

    /**
     * @return Current leaners.
     */
    List<Peer> learners();

    /** */
    public interface Builder {
        /**
         * @param peers Current peers.
         * @return The builder.
         */
        Builder peers(List<Peer> peers);

        /**
         * @param learners Current learners.
         * @return The builder.
         */
        Builder learners(List<Peer> learners);

        /**
         * @return The complete message.
         * @throws IllegalStateException If the message is not in valid state.
         */
        GetPeersResponse build();
    }
}
