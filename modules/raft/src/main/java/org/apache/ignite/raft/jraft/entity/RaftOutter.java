/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: raft.proto

package org.apache.ignite.raft.jraft.entity;

import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.rpc.MessageBuilderFactory;

public final class RaftOutter {
    public interface EntryMeta extends Message {
        static Builder newBuilder() {
            return MessageBuilderFactory.DEFAULT.createEntryMeta();
        }

        long getTerm();

        EnumOutter.EntryType getType();

        java.util.List<String> getPeersList();

        int getPeersCount();

        String getPeers(int index);

        long getDataLen();

        java.util.List<String> getOldPeersList();

        int getOldPeersCount();

        String getOldPeers(int index);

        long getChecksum();

        java.util.List<String> getLearnersList();

        int getLearnersCount();

        String getLearners(int index);

        java.util.List<String> getOldLearnersList();

        int getOldLearnersCount();

        String getOldLearners(int index);

        interface Builder {
            EntryMeta build();

            Builder setTerm(long term);

            Builder setChecksum(long checksum);

            Builder setType(EnumOutter.EntryType type);

            Builder setDataLen(int remaining);

            Builder addPeers(String peerId);

            Builder addOldPeers(String oldPeerId);

            Builder addLearners(String learnerId);

            Builder addOldLearners(String oldLearnerId);
        }
    }

    public interface SnapshotMeta extends Message {
        static Builder newBuilder() {
            return MessageBuilderFactory.DEFAULT.createSnapshotMeta();
        }

        long getLastIncludedIndex();

        long getLastIncludedTerm();

        java.util.List<String> getPeersList();

        int getPeersCount();

        String getPeers(int index);

        java.util.List<String> getOldPeersList();

        int getOldPeersCount();

        String getOldPeers(int index);

        java.util.List<String> getLearnersList();

        int getLearnersCount();

        String getLearners(int index);

        java.util.List<String> getOldLearnersList();

        int getOldLearnersCount();

        String getOldLearners(int index);

        interface Builder {
            SnapshotMeta build();

            Builder setLastIncludedIndex(long lastAppliedIndex);

            Builder setLastIncludedTerm(long lastAppliedTerm);

            Builder addPeers(String peerId);

            Builder addLearners(String learnerId);

            Builder addOldPeers(String oldPeerId);

            Builder addOldLearners(String oldLearnerId);
        }
    }
}
