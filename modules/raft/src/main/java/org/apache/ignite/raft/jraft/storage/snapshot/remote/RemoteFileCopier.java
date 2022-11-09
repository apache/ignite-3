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
package org.apache.ignite.raft.jraft.storage.snapshot.remote;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.raft.jraft.core.Scheduler;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.option.CopyOptions;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.option.SnapshotCopierOptions;
import org.apache.ignite.raft.jraft.rpc.GetFileRequestBuilder;
import org.apache.ignite.raft.jraft.rpc.RaftClientService;
import org.apache.ignite.raft.jraft.storage.SnapshotThrottle;
import org.apache.ignite.raft.jraft.storage.snapshot.Snapshot;
import org.apache.ignite.raft.jraft.util.ByteBufferCollector;
import org.apache.ignite.raft.jraft.util.OnlyForTest;
import org.apache.ignite.raft.jraft.util.Utils;

/**
 * Remote file copier
 */
public class RemoteFileCopier {

    private static final IgniteLogger LOG = Loggers.forClass(RemoteFileCopier.class);

    private long readId;
    private RaftClientService rpcService;
    private PeerId peerId;
    private RaftOptions raftOptions;
    private NodeOptions nodeOptions;
    private Scheduler timerManager;
    private SnapshotThrottle snapshotThrottle;

    @OnlyForTest
    long getReaderId() {
        return this.readId;
    }

    @OnlyForTest
    PeerId getPeerId() {
        return this.peerId;
    }

    public boolean init(String uri, final SnapshotThrottle snapshotThrottle, final SnapshotCopierOptions opts) {
        this.rpcService = opts.getRaftClientService();
        this.timerManager = opts.getTimerManager();
        this.raftOptions = opts.getRaftOptions();
        this.nodeOptions = opts.getNodeOptions();
        this.snapshotThrottle = snapshotThrottle;

        final int prefixSize = Snapshot.REMOTE_SNAPSHOT_URI_SCHEME.length();
        if (uri == null || !uri.startsWith(Snapshot.REMOTE_SNAPSHOT_URI_SCHEME)) {
            LOG.error("Invalid uri {}.", uri);
            return false;
        }
        uri = uri.substring(prefixSize);
        final int slasPos = uri.indexOf('/');
        final String peerId = uri.substring(0, slasPos);
        uri = uri.substring(slasPos + 1);

        try {
            this.readId = Long.parseLong(uri);
            this.peerId = PeerId.parsePeer(peerId);
        }
        catch (final Exception e) {
            LOG.error("Fail to parse readerId or endpoint.", e);
            return false;
        }
        if (!this.rpcService.connect(this.peerId)) {
            LOG.error("Fail to init channel to {}.", this.peerId);
            return false;
        }

        return true;
    }

    /**
     * Copy `source` from remote to local dest.
     *
     * @param source source from remote
     * @param destPath local path
     * @param opts options of copy
     * @return true if copy success
     */
    public boolean copyToFile(final String source, final String destPath, final CopyOptions opts) throws IOException,
        InterruptedException {
        final Session session = startCopyToFile(source, destPath, opts);
        if (session == null) {
            return false;
        }
        try {
            session.join();
            return session.status().isOk();
        }
        finally {
            Utils.closeQuietly(session);
        }
    }

    public Session startCopyToFile(final String source, final String destPath, final CopyOptions opts)
        throws IOException {
        final File file = new File(destPath);

        // delete exists file.
        if (file.exists()) {
            if (!file.delete()) {
                LOG.error("Fail to delete destPath: {}.", destPath);
                return null;
            }
        }

        final OutputStream out = new BufferedOutputStream(new FileOutputStream(file, false) {

            @Override
            public void close() throws IOException {
                getFD().sync();
                super.close();
            }
        });
        final CopySession session = newCopySession(source);
        session.setOutputStream(out);
        session.setDestPath(destPath);
        session.setDestBuf(null);
        if (opts != null) {
            session.setCopyOptions(opts);
        }
        session.sendNextRpc();
        return session;
    }

    private CopySession newCopySession(final String source) {
        final GetFileRequestBuilder reqBuilder = raftOptions.getRaftMessagesFactory()
            .getFileRequest()
            .filename(source)
            .readerId(this.readId);
        return new CopySession(this.rpcService, this.timerManager, this.snapshotThrottle, this.raftOptions, this.nodeOptions, reqBuilder,
            this.peerId);
    }

    /**
     * Copy `source` from remote to  buffer.
     *
     * @param source source from remote
     * @param destBuf buffer of dest
     * @param opt options of copy
     * @return true if copy success
     */
    public boolean copy2IoBuffer(final String source, final ByteBufferCollector destBuf, final CopyOptions opt)
        throws InterruptedException {
        final Session session = startCopy2IoBuffer(source, destBuf, opt);
        if (session == null) {
            return false;
        }
        try {
            session.join();
            return session.status().isOk();
        }
        finally {
            Utils.closeQuietly(session);
        }
    }

    public Session startCopy2IoBuffer(final String source, final ByteBufferCollector destBuf, final CopyOptions opts) {
        final CopySession session = newCopySession(source);
        session.setOutputStream(null);
        session.setDestBuf(destBuf);
        if (opts != null) {
            session.setCopyOptions(opts);
        }
        session.sendNextRpc();
        return session;
    }
}
