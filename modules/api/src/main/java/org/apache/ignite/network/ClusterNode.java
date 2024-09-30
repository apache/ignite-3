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

package org.apache.ignite.network;

import java.io.Serializable;
import java.util.UUID;
import org.jetbrains.annotations.Nullable;

/**
 * Representation of a node in a cluster.
 */
public interface ClusterNode extends Serializable {
    /**
     * Returns the node's local ID.
     *
     * @return Node's local ID.
     */
    UUID id();

    /**
     * Returns the unique name (consistent ID) of the node in the cluster. Does not change between restarts.
     *
     * @return Unique name of a cluster member.
     */
    String name();

    /**
     * Returns the network address of the node.
     *
     * @return Network address of the node.
     */
    NetworkAddress address();

    /**
     * Returns the metadata of the node.
     *
     * @return Metadata of the node.
     */
    @Nullable
    NodeMetadata nodeMetadata();
}
