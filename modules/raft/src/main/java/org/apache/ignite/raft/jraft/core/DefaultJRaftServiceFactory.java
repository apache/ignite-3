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
package org.apache.ignite.raft.jraft.core;

import org.apache.ignite.raft.jraft.JRaftServiceFactory;
import org.apache.ignite.raft.jraft.entity.codec.LogEntryCodecFactory;
import org.apache.ignite.raft.jraft.entity.codec.v1.LogEntryV1CodecFactory;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.storage.LogStorage;
import org.apache.ignite.raft.jraft.storage.RaftMetaStorage;
import org.apache.ignite.raft.jraft.storage.SnapshotStorage;
import org.apache.ignite.raft.jraft.storage.impl.LocalLogStorage;
import org.apache.ignite.raft.jraft.storage.impl.LocalRaftMetaStorage;
import org.apache.ignite.raft.jraft.storage.snapshot.local.LocalSnapshotStorage;
import org.apache.ignite.raft.jraft.util.Requires;
import org.apache.ignite.raft.jraft.util.SPI;
import org.apache.ignite.raft.jraft.util.StringUtils;

/**
 * The default factory for JRaft services.
 * @author boyan(boyan@antfin.com)
 * @since 1.2.6
 *
 */
@SPI
public class DefaultJRaftServiceFactory implements JRaftServiceFactory {

    public static DefaultJRaftServiceFactory newInstance() {
        return new DefaultJRaftServiceFactory();
    }

    @Override
    public LogStorage createLogStorage(final String uri, final RaftOptions raftOptions) {
        Requires.requireTrue(StringUtils.isNotBlank(uri), "Blank log storage uri.");
        return new LocalLogStorage(uri, raftOptions);
    }

    @Override
    public SnapshotStorage createSnapshotStorage(final String uri, final RaftOptions raftOptions) {
        Requires.requireTrue(!StringUtils.isBlank(uri), "Blank snapshot storage uri.");
        return new LocalSnapshotStorage(uri, raftOptions);
    }

    @Override
    public RaftMetaStorage createRaftMetaStorage(final String uri, final RaftOptions raftOptions) {
        Requires.requireTrue(!StringUtils.isBlank(uri), "Blank raft meta storage uri.");
        return new LocalRaftMetaStorage(uri, raftOptions);
    }

    @Override
    public LogEntryCodecFactory createLogEntryCodecFactory() {
        return LogEntryV1CodecFactory.getInstance();
    }
}
