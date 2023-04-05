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

package org.apache.ignite.internal.metastorage.command.cursor;

import org.apache.ignite.internal.metastorage.command.MetastorageCommandsMessageGroup;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.network.annotations.Transferable;

/**
 * Command that closes all cursors for the specified node id. Common use case for a given command is to close cursors for the node that left
 * topology.
 */
@Transferable(MetastorageCommandsMessageGroup.CLOSE_ALL_CURSORS)
public interface CloseAllCursorsCommand extends WriteCommand {
    /**
     * Returns cursor id.
     */
    String nodeId();
}
