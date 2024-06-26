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
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.network.annotations.Transferable;
import org.apache.ignite.internal.raft.ReadCommand;

/**
 * Get all command for MetaStorageCommandListener that retrieves entries for given keys and the revision upper bound, if latter is present.
 */
// TODO: IGNITE-22583 тут
@Transferable(MetastorageCommandsMessageGroup.GET_ALL)
public interface GetAllCommand extends ReadCommand {
    /**
     * Returns the list of keys.
     */
    List<byte[]> keys();

    /**
     * Returns the upper bound for entry revisions or {@link MetaStorageManager#LATEST_REVISION} for no revision bound.
     */
    long revision();

    /**
     * Static constructor.
     *
     * @param commandsFactory Commands factory.
     * @param keys The collection of keys. Couldn't be {@code null} or empty. Collection elements couldn't be {@code null}.
     * @param revUpperBound The upper bound for entry revisions. Must be positive.
     */
    static GetAllCommand getAllCommand(MetaStorageCommandsFactory commandsFactory, Set<ByteArray> keys, long revUpperBound) {
        List<byte[]> keysList = new ArrayList<>(keys.size());

        for (ByteArray key : keys) {
            keysList.add(key.bytes());
        }

        return commandsFactory.getAllCommand().keys(keysList).revision(revUpperBound).build();
    }
}
