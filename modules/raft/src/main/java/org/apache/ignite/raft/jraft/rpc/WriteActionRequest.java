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

package org.apache.ignite.raft.jraft.rpc;

import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.network.annotations.Transferable;
import org.apache.ignite.internal.network.annotations.Transient;
import org.apache.ignite.raft.jraft.RaftMessageGroup.RpcActionMessageGroup;
import org.jetbrains.annotations.Nullable;

/**
 * Submit a  write action to a replication group.
 */
@Transferable(RpcActionMessageGroup.WRITE_ACTION_REQUEST)
public interface WriteActionRequest extends ActionRequest {
    /**
     * @return Serialized action's command. Specific serialization format may differ from group to group.
     */
    byte[] command();

    /**
     * @return Original non-serialized command, if available. {@code null} if not. This field is used to avoid {@link #command()}
     * deserialization in cases, where deserialized instance is already available. Typical situation for it is command's creation, where
     * command is explicitly serialized into {@code byte[]} before building the message.
     */
    @Transient
    @Nullable WriteCommand deserializedCommand();
}
