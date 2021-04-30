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

package org.apache.ignite.metastorage.common.raft;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.internal.metastorage.common.command.GetAllCommand;
import org.apache.ignite.internal.metastorage.common.command.GetAndPutAllCommand;
import org.apache.ignite.internal.metastorage.common.command.GetAndPutCommand;
import org.apache.ignite.internal.metastorage.common.command.GetAndRemoveAllCommand;
import org.apache.ignite.internal.metastorage.common.command.GetAndRemoveCommand;
import org.apache.ignite.internal.metastorage.common.command.GetCommand;
import org.apache.ignite.internal.metastorage.common.command.PutAllCommand;
import org.apache.ignite.internal.metastorage.common.command.PutCommand;
import org.apache.ignite.internal.metastorage.common.command.RemoveAllCommand;
import org.apache.ignite.internal.metastorage.common.command.RemoveCommand;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.metastorage.common.Entry;
import org.apache.ignite.metastorage.common.Key;
import org.apache.ignite.metastorage.common.KeyValueStorage;
import org.apache.ignite.raft.client.ReadCommand;
import org.apache.ignite.raft.client.WriteCommand;
import org.apache.ignite.raft.client.service.CommandClosure;
import org.apache.ignite.raft.client.service.RaftGroupCommandListener;

/**
 * Meta storage command listener aka mata storage raft state machine.
 */
public class MetaStorageCommandListener implements RaftGroupCommandListener {
    /** The logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(MetaStorageCommandListener.class);

    /** Storage. */
    private final KeyValueStorage storage;

    /**
     * @param storage Storage.
     */
    public MetaStorageCommandListener(KeyValueStorage storage) {
        this.storage = storage;
    }

    /** {@inheritDoc} */
    @Override public void onRead(Iterator<CommandClosure<ReadCommand>> iterator) {
        while (iterator.hasNext()) {
            CommandClosure<ReadCommand> clo = iterator.next();

            try {
                if (clo.command() instanceof GetCommand) {
                    GetCommand getCmd = (GetCommand)clo.command();

                    if (getCmd.revision() != null)
                        clo.success(storage.get(getCmd.key().bytes(), getCmd.revision()));
                    else
                        clo.success(storage.get(getCmd.key().bytes()));
                }
                else if (clo.command() instanceof GetAllCommand) {
                    GetAllCommand getAllCmd = (GetAllCommand)clo.command();

                    if (getAllCmd.revision() != null) {
                        clo.success(storage.getAll(
                            getAllCmd.keys().stream().map(Key::bytes).collect(Collectors.toList()),
                            getAllCmd.revision()
                        ).stream().collect(Collectors.toMap(Entry::key, Function.identity())));
                    }
                    else {
                        clo.success(storage.getAll(
                            getAllCmd.keys().stream().map(Key::bytes).collect(Collectors.toList())).
                            stream().collect(Collectors.toMap(Entry::key, Function.identity())));
                    }
                }
                else
                    assert false : "Command was not found [cmd=" + clo.command() + ']';
            }
            catch (Throwable e) {
                LOG.error("Unable to evaluate command [cmd=" + clo.command() + ']', e);

                clo.failure(e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void onWrite(Iterator<CommandClosure<WriteCommand>> iterator) {
        while (iterator.hasNext()) {
            CommandClosure<WriteCommand> clo = iterator.next();

            try {
                if (clo.command() instanceof PutCommand) {
                    PutCommand putCmd = (PutCommand)clo.command();

                    storage.put(putCmd.key().bytes(), putCmd.value());

                    clo.success(null);
                }
                else if (clo.command() instanceof GetAndPutCommand) {
                    GetAndPutCommand getAndPutCmd = (GetAndPutCommand)clo.command();

                    clo.success(storage.getAndPut(getAndPutCmd.key().bytes(), getAndPutCmd.value()));
                }
                else if (clo.command() instanceof PutAllCommand) {
                    PutAllCommand putAllCmd = (PutAllCommand)clo.command();

                    storage.putAll(
                        putAllCmd.values().keySet().stream().map(Key::bytes).collect(Collectors.toList()),
                        new ArrayList<>(putAllCmd.values().values()));

                    clo.success(null);
                }
                else if (clo.command() instanceof GetAndPutAllCommand) {
                    GetAndPutAllCommand getAndPutAllCmd = (GetAndPutAllCommand)clo.command();

                    clo.success(storage.getAndPutAll(
                        getAndPutAllCmd.values().keySet().stream().map(Key::bytes).collect(Collectors.toList()),
                        new ArrayList<>(getAndPutAllCmd.values().values())));
                }
                else if (clo.command() instanceof RemoveCommand) {
                    RemoveCommand rmvCmd = (RemoveCommand)clo.command();

                    storage.remove(rmvCmd.key().bytes());

                    clo.success(null);
                }
                else if (clo.command() instanceof GetAndRemoveCommand) {
                    GetAndRemoveCommand getAndRmvCmd = (GetAndRemoveCommand)clo.command();

                    clo.success(storage.getAndRemove(getAndRmvCmd.key().bytes()));
                }
                else if (clo.command() instanceof RemoveAllCommand) {
                    RemoveAllCommand rmvAllCmd = (RemoveAllCommand)clo.command();

                    storage.removeAll(rmvAllCmd.keys().stream().map(Key::bytes).collect(Collectors.toList()));

                    clo.success(null);
                }
                else if (clo.command() instanceof GetAndRemoveAllCommand) {
                    GetAndRemoveAllCommand getAndRmvAllCmd = (GetAndRemoveAllCommand)clo.command();

                    clo.success(storage.getAndRemoveAll(
                        getAndRmvAllCmd.keys().stream().map(Key::bytes).collect(Collectors.toList())));
                }
                else
                    assert false : "Command was not found [cmd=" + clo.command() + ']';
            }
            catch (Throwable e) {
                LOG.error("Unable to evaluate command [cmd=" + clo.command() + ']', e);

                clo.failure(e);
            }
        }
    }
}
