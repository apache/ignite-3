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
// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: local_storage.proto

package org.apache.ignite.raft.jraft.entity;

import java.util.List;
import org.apache.ignite.internal.network.annotations.Transferable;
import org.apache.ignite.raft.jraft.RaftMessageGroup;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.jetbrains.annotations.Nullable;

public final class LocalStorageOutter {
    @Transferable(value = RaftMessageGroup.RaftOutterMessageGroup.STABLE_PB_META)
    public interface StablePBMeta extends Message {
        long term();

        String votedFor();
    }

    @Transferable(value = RaftMessageGroup.RaftOutterMessageGroup.LOCAL_SNAPSHOT_PB_META)
    public interface LocalSnapshotPbMeta extends Message {
        @Nullable
        RaftOutter.SnapshotMeta meta();

        List<LocalStorageOutter.LocalSnapshotPbMeta.File> filesList();

        @Transferable(value = RaftMessageGroup.RaftOutterMessageGroup.LOCAL_SNAPSHOT_META_FILE)
        interface File extends Message {
            String name();

            LocalFileMetaOutter.LocalFileMeta meta();
        }
    }
}
