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

package org.apache.ignite.internal.metastorage;

import java.io.Serializable;
import java.util.UUID;
import org.apache.ignite.internal.metastorage.dsl.MetaStorageMessageGroup;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.annotations.Transferable;

/**
 * Command id. It consists of node id and a counter that is generated on the node and is unique for that node, so the whole command id
 * would be unique cluster-wide.
 */
@Transferable(MetaStorageMessageGroup.COMMAND_ID)
public interface CommandId extends NetworkMessage, Serializable {
    UUID nodeId();

    long counter();
}
