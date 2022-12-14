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
import java.util.Map;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.network.annotations.Transferable;

/**
 * Put all command for MetaStorageCommandListener that inserts or updates entries with given keys and given values.
 */
@Transferable(MetastorageCommandsMessageGroup.PUT_ALL)
public interface PutAllCommand extends WriteCommand {
    /**
     * Returns entries keys.
     */
    List<byte[]> keys();

    /**
     * Returns entries values.
     */
    List<byte[]> values();

    /**
     * Static constructor.
     *
     * @param commandsFactory Commands factory.
     * @param vals The map of keys and corresponding values. Couldn't be {@code null} or empty.
     */
    static PutAllCommand putAllCommand(MetaStorageCommandsFactory commandsFactory, Map<ByteArray, byte[]> vals) {
        assert !vals.isEmpty();

        int size = vals.size();

        List<byte[]> keys = new ArrayList<>(size);

        List<byte[]> values = new ArrayList<>(size);

        for (Map.Entry<ByteArray, byte[]> e : vals.entrySet()) {
            byte[] key = e.getKey().bytes();

            byte[] val = e.getValue();

            assert key != null : "Key could not be null.";
            assert val != null : "Value could not be null.";

            keys.add(key);

            values.add(val);
        }

        return commandsFactory.putAllCommand().keys(keys).values(values).build();
    }
}
