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
package org.apache.ignite.raft.jraft.rpc;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import org.apache.ignite.raft.jraft.JRaftUtils;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.error.InvokeTimeoutException;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.error.RemotingException;
import org.apache.ignite.raft.jraft.option.RpcOptions;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.ErrorResponse;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.PingRequest;
import org.apache.ignite.raft.jraft.rpc.impl.AbstractClientService;
import org.apache.ignite.raft.jraft.test.TestUtils;
import org.apache.ignite.raft.jraft.util.Endpoint;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

@RunWith(value = MockitoJUnitRunner.class)
public class AbstractClientServiceTest {
    private RpcOptions rpcOptions;
    private MockClientService clientService;
    @Mock
    private RpcClient rpcClient;
    private RpcResponseFactory rpcResponseFactory = RaftRpcFactory.DEFAULT;
    private final Endpoint endpoint = new Endpoint("localhost", 8081);

    @Before
    public void setup() throws Exception {
        this.rpcOptions = new RpcOptions();
        this.rpcOptions.setClientExecutor(JRaftUtils.createClientExecutor(this.rpcOptions, "unittest"));
        this.clientService = new MockClientService();
        when(this.rpcClient.invokeAsync(any(), any(), any(), any(), anyLong())).thenReturn(new CompletableFuture<>());
        this.rpcOptions.setRpcClient(this.rpcClient);
        assertTrue(this.clientService.init(this.rpcOptions));
    }

    static class MockRpcResponseClosure<T extends Message> extends RpcResponseClosureAdapter<T> {
        CountDownLatch latch = new CountDownLatch(1);

        Status status;

        @Override
        public void run(final Status status) {
            this.status = status;
            this.latch.countDown();
        }
    }

    @Test
    public void testInvokeWithDoneOK() throws Exception {
        ArgumentCaptor<InvokeCallback> callbackArg = ArgumentCaptor.forClass(InvokeCallback.class);
        PingRequest request = TestUtils.createPingRequest();

        MockRpcResponseClosure<ErrorResponse> done = new MockRpcResponseClosure<>();
        Future<Message> future = this.clientService.invokeWithDone(this.endpoint, request, done, -1);
        Mockito.verify(this.rpcClient).invokeAsync(eq(this.endpoint), eq(request), Mockito.any(),
            callbackArg.capture(), eq((long) this.rpcOptions.getRpcDefaultTimeout()));
        InvokeCallback cb = callbackArg.getValue();
        assertNotNull(cb);
        assertNotNull(future);

        assertNull(done.getResponse());
        assertNull(done.status);
        assertFalse(future.isDone());

        ErrorResponse response = (ErrorResponse) this.rpcResponseFactory.newResponse(null, Status.OK());
        cb.complete(response, null);

        done.latch.await();
        assertNotNull(done.status);
        assertEquals(0, done.status.getCode());
    }

    @Test
    public void testInvokeWithDoneException() throws Exception {
        InvokeContext invokeCtx = new InvokeContext();
        ArgumentCaptor<InvokeCallback> callbackArg = ArgumentCaptor.forClass(InvokeCallback.class);
        PingRequest request = TestUtils.createPingRequest();

        Mockito
            .doThrow(new RemotingException())
            .when(this.rpcClient)
            .invokeAsync(eq(this.endpoint), eq(request), eq(invokeCtx), callbackArg.capture(),
                eq((long) this.rpcOptions.getRpcDefaultTimeout()));

        MockRpcResponseClosure<ErrorResponse> done = new MockRpcResponseClosure<>();
        Future<Message> future = this.clientService.invokeWithDone(this.endpoint, request, invokeCtx, done, -1);
        InvokeCallback cb = callbackArg.getValue();
        assertNotNull(cb);
        assertNotNull(future);

        assertTrue(future.isDone());

        done.latch.await();
        assertNotNull(done.status);
        assertEquals(RaftError.EINTERNAL.getNumber(), done.status.getCode());
    }

    @Test
    public void testInvokeWithDoneOnException() throws Exception {
        InvokeContext invokeCtx = new InvokeContext();
        ArgumentCaptor<InvokeCallback> callbackArg = ArgumentCaptor.forClass(InvokeCallback.class);
        PingRequest request = TestUtils.createPingRequest();

        MockRpcResponseClosure<ErrorResponse> done = new MockRpcResponseClosure<>();
        Future<Message> future = this.clientService.invokeWithDone(this.endpoint, request, invokeCtx, done, -1);
        Mockito.verify(this.rpcClient).invokeAsync(eq(this.endpoint), eq(request), eq(invokeCtx),
            callbackArg.capture(), eq((long) this.rpcOptions.getRpcDefaultTimeout()));
        InvokeCallback cb = callbackArg.getValue();
        assertNotNull(cb);
        assertNotNull(future);

        assertNull(done.getResponse());
        assertNull(done.status);
        assertFalse(future.isDone());

        cb.complete(null, new InvokeTimeoutException());

        done.latch.await();
        assertNotNull(done.status);
        assertEquals(RaftError.ETIMEDOUT.getNumber(), done.status.getCode());
    }

    @Test
    public void testInvokeWithDoneOnErrorResponse() throws Exception {
        final InvokeContext invokeCtx = new InvokeContext();
        final ArgumentCaptor<InvokeCallback> callbackArg = ArgumentCaptor.forClass(InvokeCallback.class);
        final CliRequests.GetPeersRequest request = CliRequests.GetPeersRequest.newBuilder() //
            .setGroupId("id") //
            .setLeaderId("127.0.0.1:8001") //
            .build();

        MockRpcResponseClosure<ErrorResponse> done = new MockRpcResponseClosure<>();
        Future<Message> future = this.clientService.invokeWithDone(this.endpoint, request, invokeCtx, done, -1);
        Mockito.verify(this.rpcClient).invokeAsync(eq(this.endpoint), eq(request), eq(invokeCtx),
            callbackArg.capture(), eq((long) this.rpcOptions.getRpcDefaultTimeout()));
        InvokeCallback cb = callbackArg.getValue();
        assertNotNull(cb);
        assertNotNull(future);

        assertNull(done.getResponse());
        assertNull(done.status);
        assertFalse(future.isDone());

        final Message resp = this.rpcResponseFactory.newResponse(CliRequests.GetPeersResponse.getDefaultInstance(),
            new Status(-1, "failed"));
        cb.complete(resp, null);

        done.latch.await();
        assertNotNull(done.status);
        assertTrue(!done.status.isOk());
        assertEquals(done.status.getErrorMsg(), "failed");
    }

    static class MockClientService extends AbstractClientService {
    }
}
