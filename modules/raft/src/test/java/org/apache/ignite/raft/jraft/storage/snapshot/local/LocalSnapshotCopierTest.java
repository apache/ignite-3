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
package org.apache.ignite.raft.jraft.storage.snapshot.local;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import org.apache.ignite.raft.jraft.JRaftUtils;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.core.Scheduler;
import org.apache.ignite.raft.jraft.core.TimerManager;
import org.apache.ignite.raft.jraft.entity.LocalFileMetaOutter;
import org.apache.ignite.raft.jraft.entity.RaftOutter;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.option.CopyOptions;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.option.SnapshotCopierOptions;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.rpc.RaftClientService;
import org.apache.ignite.raft.jraft.rpc.RpcRequests;
import org.apache.ignite.raft.jraft.rpc.RpcResponseClosure;
import org.apache.ignite.raft.jraft.storage.BaseStorageTest;
import org.apache.ignite.raft.jraft.storage.snapshot.Snapshot;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotReader;
import org.apache.ignite.raft.jraft.util.ByteString;
import org.apache.ignite.raft.jraft.util.Endpoint;
import org.apache.ignite.raft.jraft.util.Utils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;

@RunWith(value = MockitoJUnitRunner.class)
public class LocalSnapshotCopierTest extends BaseStorageTest {
    private LocalSnapshotCopier copier;
    @Mock
    private RaftClientService raftClientService;
    private String uri;
    private final String hostPort = "localhost:8081";
    private final int readerId = 99;
    private CopyOptions copyOpts;
    private LocalSnapshotMetaTable table;
    private LocalSnapshotWriter writer;
    private LocalSnapshotReader reader;
    private RaftOptions raftOptions;
    @Mock
    private LocalSnapshotStorage snapshotStorage;
    private Scheduler timerManager;

    @Override
    @Before
    public void setup() throws Exception {
        super.setup();
        this.timerManager = new TimerManager(5);
        this.raftOptions = new RaftOptions();
        this.writer = new LocalSnapshotWriter(this.path, this.snapshotStorage, this.raftOptions);
        this.reader = new LocalSnapshotReader(this.snapshotStorage, null, new Endpoint("localhost", 8081),
            this.raftOptions, this.path);

        Mockito.when(this.snapshotStorage.open()).thenReturn(this.reader);
        Mockito.when(this.snapshotStorage.create(true)).thenReturn(this.writer);

        this.table = new LocalSnapshotMetaTable(this.raftOptions);
        this.table.addFile("testFile", LocalFileMetaOutter.LocalFileMeta.newBuilder().setChecksum("test").build());
        this.table.setMeta(RaftOutter.SnapshotMeta.newBuilder().setLastIncludedIndex(1).setLastIncludedTerm(1).build());
        this.uri = "remote://" + this.hostPort + "/" + this.readerId;
        this.copier = new LocalSnapshotCopier();
        this.copyOpts = new CopyOptions();
        Mockito.when(this.raftClientService.connect(new Endpoint("localhost", 8081))).thenReturn(true);
        NodeOptions nodeOptions = new NodeOptions();
        nodeOptions.setCommonExecutor(JRaftUtils.createExecutor("test-executor", Utils.cpus()));
        assertTrue(this.copier.init(this.uri, new SnapshotCopierOptions(this.raftClientService, this.timerManager,
            this.raftOptions, nodeOptions)));
        this.copier.setStorage(this.snapshotStorage);
    }

    @Override
    @After
    public void teardown() throws Exception {
        super.teardown();
        this.copier.close();
        this.timerManager.shutdown();
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void testCancelByRemote() throws Exception {
        final CompletableFuture<Message> future = new CompletableFuture<>();
        final RpcRequests.GetFileRequest.Builder rb = RpcRequests.GetFileRequest.newBuilder().setReaderId(99)
            .setFilename(Snapshot.JRAFT_SNAPSHOT_META_FILE).setCount(Integer.MAX_VALUE).setOffset(0)
            .setReadPartly(true);

        //mock get metadata
        final ArgumentCaptor<RpcResponseClosure> argument = ArgumentCaptor.forClass(RpcResponseClosure.class);
        Mockito.when(
            this.raftClientService.getFile(eq(new Endpoint("localhost", 8081)), eq(rb.build()),
                eq(this.copyOpts.getTimeoutMs()), argument.capture())).thenReturn(future);
        this.copier.start();
        Thread.sleep(500);
        final RpcResponseClosure<RpcRequests.GetFileResponse> closure = argument.getValue();

        closure.run(new Status(RaftError.ECANCELED, "test cancel"));

        this.copier.join();
        //start timer
        final SnapshotReader reader = this.copier.getReader();
        assertNull(reader);
        Assert.assertEquals(RaftError.ECANCELED.getNumber(), this.copier.getCode());
        Assert.assertEquals("test cancel", this.copier.getErrorMsg());
    }

    @Test
    public void testInterrupt() throws Exception {
        final CompletableFuture<Message> future = new CompletableFuture<>();
        final RpcRequests.GetFileRequest.Builder rb = RpcRequests.GetFileRequest.newBuilder().setReaderId(99)
            .setFilename(Snapshot.JRAFT_SNAPSHOT_META_FILE).setCount(Integer.MAX_VALUE).setOffset(0)
            .setReadPartly(true);

        //mock get metadata
        final ArgumentCaptor<RpcResponseClosure> argument = ArgumentCaptor.forClass(RpcResponseClosure.class);
        Mockito.when(
            this.raftClientService.getFile(eq(new Endpoint("localhost", 8081)), eq(rb.build()),
                eq(this.copyOpts.getTimeoutMs()), argument.capture())).thenReturn(future);
        this.copier.start();
        Thread.sleep(10);

        Utils.runInThread(Executors.newSingleThreadExecutor(), new Runnable() {
            @Override
            public void run() {
                LocalSnapshotCopierTest.this.copier.cancel();
            }
        });

        this.copier.join();
        //start timer
        final SnapshotReader reader = this.copier.getReader();
        assertNull(reader);
        Assert.assertEquals(RaftError.ECANCELED.getNumber(), this.copier.getCode());
        Assert.assertEquals("Cancel the copier manually.", this.copier.getErrorMsg());
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void testStartJoinFinishOK() throws Exception {
        final CompletableFuture<Message> future = new CompletableFuture<>();
        final RpcRequests.GetFileRequest.Builder rb = RpcRequests.GetFileRequest.newBuilder().setReaderId(99)
            .setFilename(Snapshot.JRAFT_SNAPSHOT_META_FILE).setCount(Integer.MAX_VALUE).setOffset(0)
            .setReadPartly(true);

        //mock get metadata
        ArgumentCaptor<RpcResponseClosure> argument = ArgumentCaptor.forClass(RpcResponseClosure.class);
        Mockito.when(
            this.raftClientService.getFile(eq(new Endpoint("localhost", 8081)), eq(rb.build()),
                eq(this.copyOpts.getTimeoutMs()), argument.capture())).thenReturn(future);
        this.copier.start();
        Thread.sleep(500);
        RpcResponseClosure<RpcRequests.GetFileResponse> closure = argument.getValue();
        final ByteBuffer metaBuf = this.table.saveToByteBufferAsRemote();
        closure.setResponse(RpcRequests.GetFileResponse.newBuilder().setReadSize(metaBuf.remaining()).setEof(true)
            .setData(new ByteString(metaBuf)).build());

        //mock get file
        argument = ArgumentCaptor.forClass(RpcResponseClosure.class);
        rb.setFilename("testFile");
        rb.setCount(this.raftOptions.getMaxByteCountPerRpc());
        Mockito.when(
            this.raftClientService.getFile(eq(new Endpoint("localhost", 8081)), eq(rb.build()),
                eq(this.copyOpts.getTimeoutMs()), argument.capture())).thenReturn(future);

        closure.run(Status.OK());

        Thread.sleep(500);
        closure = argument.getValue();
        closure.setResponse(RpcRequests.GetFileResponse.newBuilder().setReadSize(100).setEof(true)
            .setData(new ByteString(new byte[100])).build());
        closure.run(Status.OK());
        this.copier.join();
        final SnapshotReader reader = this.copier.getReader();
        assertSame(this.reader, reader);
        assertEquals(1, this.writer.listFiles().size());
        assertTrue(this.writer.listFiles().contains("testFile"));
    }
}
