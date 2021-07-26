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

package org.apache.ignite.internal.metastorage.message;

import org.apache.ignite.network.annotations.MessageGroup;

/**
 * Message group for the {@code ignite-metastorage} module.
 */
@MessageGroup(groupType = 5, groupName = "MetastorageMessages")
public class MetastorageMessages {
    /**
     * Message type of the {@link MetastorageNodesRequest}.
     */
    public static final short METASTORAGE_NODES_REQUEST = 0;

    /**
     * Message type of the {@link MetastorageNodesResponse}.
     */
    public static final short METASTORAGE_NODES_RESPONSE = 1;

    /**
     * Message type of the {@link MetastorageNotReadyResponse}.
     */
    public static final short METASTORAGE_NOT_READY_RESPONSE = 2;
}
