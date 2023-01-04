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

package org.apache.ignite.internal.metastorage.command;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.network.annotations.Transferable;

/**
 * Watch command for MetaStorageCommandListener that subscribes on meta storage updates matching the parameters.
 */
@Transferable(MetastorageCommandsMessageGroup.WATCH_EXACT_KEYS)
public interface WatchExactKeysCommand extends WriteCommand {
    /**
     * Returns the keys list. Couldn't be {@code null}.
     */
    List<byte[]> keys();

    /**
     * Returns start revision inclusive. {@code 0} - all revisions.
     */
    long revision();

    /**
     * Returns id of the node that requests range.
     */
    String requesterNodeId();

    /**
     * Returns id of cursor that is associated with the current command.
     */
    IgniteUuid cursorId();

    /**
     * Static constructor.
     *
     * @param commandsFactory Commands factory.
     * @param keys            The keys collection. Couldn't be {@code null}.
     * @param revision        Start revision inclusive. {@code 0} - all revisions.
     * @param requesterNodeId Id of the node that requests watch.
     * @param cursorId        Id of cursor that is associated with the current command.
     */
    static WatchExactKeysCommand watchExactKeysCommand(
            MetaStorageCommandsFactory commandsFactory,
            Set<ByteArray> keys,
            long revision,
            String requesterNodeId,
            IgniteUuid cursorId
    ) {
        List<byte[]> list = new ArrayList<>(keys.size());

        for (ByteArray key : keys) {
            list.add(key.bytes());
        }

        return commandsFactory.watchExactKeysCommand()
                .keys(list)
                .revision(revision)
                .requesterNodeId(requesterNodeId)
                .cursorId(cursorId)
                .build();
    }
}
