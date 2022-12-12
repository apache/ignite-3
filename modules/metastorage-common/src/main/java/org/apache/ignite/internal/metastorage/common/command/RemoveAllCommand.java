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

package org.apache.ignite.internal.metastorage.common.command;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.network.annotations.Transferable;

/**
 * Remove all command for MetaStorageCommandListener that removes entries for given keys.
 */
@Transferable(MetastorageCommandsMessageGroup.REMOVE_ALL)
public interface RemoveAllCommand extends WriteCommand {
    /**
     * Returns the keys list. Couldn't be {@code null}.
     */
    List<byte[]> keys();

    /**
     * Static constructor.
     *
     * @param commandsFactory Commands factory.
     * @param keys The keys collection. Couldn't be {@code null}.
     */
    static RemoveAllCommand removeAllCommand(MetaStorageCommandsFactory commandsFactory, Set<ByteArray> keys) {
        List<byte[]> list = new ArrayList<>(keys.size());

        for (ByteArray key : keys) {
            list.add(key.bytes());
        }

        return commandsFactory.removeAllCommand().keys(list).build();
    }
}
