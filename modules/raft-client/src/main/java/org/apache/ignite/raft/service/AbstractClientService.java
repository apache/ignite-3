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
package org.apache.ignite.raft.service;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import org.apache.ignite.raft.Endpoint;
import org.apache.ignite.raft.RaftError;
import org.apache.ignite.raft.Status;
import org.apache.ignite.raft.closure.RpcResponseClosure;
import org.apache.ignite.raft.rpc.InvokeCallback;
import org.apache.ignite.raft.rpc.InvokeContext;
import org.apache.ignite.raft.rpc.InvokeTimeoutException;
import org.apache.ignite.raft.rpc.Message;
import org.apache.ignite.raft.rpc.RemotingException;
import org.apache.ignite.raft.rpc.RpcClient;
import org.apache.ignite.raft.rpc.RpcOptions;
import org.apache.ignite.raft.rpc.RpcRequests.ErrorResponse;
import org.apache.ignite.raft.rpc.RpcRequests.PingRequest;
import org.apache.ignite.raft.rpc.RpcUtils;
import org.apache.ignite.raft.rpc.impl.LocalRpcClient;

/**
 * Abstract RPC client service based.
 */
public abstract class AbstractClientService implements ClientService {
    protected static final System.Logger LOG = System.getLogger(AbstractClientService.class.getName());

    protected volatile RpcClient rpcClient;
    protected RpcOptions rpcOptions;

    public RpcClient getRpcClient() {
        return this.rpcClient;
    }

    @Override
    public boolean isConnected(final Endpoint endpoint) {
        final RpcClient rc = this.rpcClient;
        return rc != null && isConnected(rc, endpoint);
    }

    private static boolean isConnected(final RpcClient rpcClient, final Endpoint endpoint) {
        return rpcClient.checkConnection(endpoint);
    }

    @Override
    public boolean checkConnection(final Endpoint endpoint, final boolean createIfAbsent) {
        final RpcClient rc = this.rpcClient;
        if (rc == null) {
            throw new IllegalStateException("Client service is uninitialized.");
        }
        return rc.checkConnection(endpoint, createIfAbsent);
    }

    @Override
    public synchronized boolean init(final RpcOptions rpcOptions) {
        if (this.rpcClient != null) {
            return true;
        }
        this.rpcOptions = rpcOptions;
        return initRpcClient();
    }

    protected boolean initRpcClient() {
        this.rpcClient = new LocalRpcClient();
        this.rpcClient.init(rpcOptions);

        return true;
    }

    @Override
    public synchronized void shutdown() {
        if (this.rpcClient != null) {
            this.rpcClient.shutdown();
            this.rpcClient = null;
        }
    }

    @Override
    public boolean connect(final Endpoint endpoint) {
        final RpcClient rc = this.rpcClient;
        if (rc == null) {
            throw new IllegalStateException("Client service is uninitialized.");
        }
        if (isConnected(rc, endpoint)) {
            return true;
        }
        try {
            final PingRequest req = PingRequest.newBuilder() //
                .setSendTimestamp(System.currentTimeMillis()) //
                .build();
            final ErrorResponse resp = (ErrorResponse) rc.invokeSync(endpoint, req,
                this.rpcOptions.getRpcConnectTimeoutMs());
            return resp.getErrorCode() == 0;
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (final RemotingException e) {
            LOG.log(System.Logger.Level.WARNING, "Fail to connect {0}, remoting exception: {1}.", endpoint, e.getMessage());
            return false;
        }
    }

    @Override
    public boolean disconnect(final Endpoint endpoint) {
        final RpcClient rc = this.rpcClient;
        if (rc == null) {
            return true;
        }
        LOG.log(System.Logger.Level.INFO, "Disconnect from {}.", endpoint);
        rc.closeConnection(endpoint);
        return true;
    }

    @Override
    public <T extends Message> Future<Message> invokeWithDone(final Endpoint endpoint, final Message request,
                                                              final RpcResponseClosure<T> done, final int timeoutMs) {
        return invokeWithDone(endpoint, request, null, done, timeoutMs, null);
    }

    public <T extends Message> Future<Message> invokeWithDone(final Endpoint endpoint, final Message request,
                                                              final InvokeContext ctx,
                                                              final RpcResponseClosure<T> done, final int timeoutMs,
                                                              final Executor rpcExecutor) {
        final RpcClient rc = this.rpcClient;
        final CompletableFuture<Message> future = new CompletableFuture<>();
        final Executor currExecutor = rpcExecutor != null ? rpcExecutor : RpcUtils.RPC_CLOSURE_EXECUTOR;
        try {
            if (rc == null) {
                future.completeExceptionally(new IllegalStateException("Client service is uninitialized."));
                // should be in another thread to avoid dead locking.
                RpcUtils.runClosureInExecutor(currExecutor, done, new Status(RaftError.EINTERNAL,
                    "Client service is uninitialized."));
                return future;
            }

            rc.invokeAsync(endpoint, request, ctx, new InvokeCallback() {
                @SuppressWarnings({"unchecked", "ConstantConditions"})
                @Override
                public void complete(final Object result, final Throwable err) {
                    if (future.isCancelled()) {
                        onCanceled(request, done);
                        return;
                    }

                    if (err == null) {
                        Status status = Status.OK();
                        Message msg;
                        if (result instanceof ErrorResponse) {
                            status = handleErrorResponse((ErrorResponse) result);
                            msg = (Message) result;
                        } else {
                            msg = (Message) result;
                        }
                        if (done != null) {
                            try {
                                if (status.isOk()) {
                                    done.setResponse((T) msg);
                                }
                                done.run(status);
                            } catch (final Throwable t) {
                                LOG.log(System.Logger.Level.WARNING, "Fail to run RpcResponseClosure, the request is {}.", request, t);
                            }
                        }
                        if (!future.isDone()) {
                            future.complete(msg);
                        }
                    } else {
                        if (done != null) {
                            try {
                                done.run(new Status(err instanceof InvokeTimeoutException ? RaftError.ETIMEDOUT
                                    : RaftError.EINTERNAL, "RPC exception:" + err.getMessage()));
                            } catch (final Throwable t) {
                                LOG.log(System.Logger.Level.WARNING, "Fail to run RpcResponseClosure, the request is {}.", request, t);
                            }
                        }
                        if (!future.isDone()) {
                            future.completeExceptionally(err);
                        }
                    }
                }

                @Override
                public Executor executor() {
                    return currExecutor;
                }
            }, timeoutMs <= 0 ? this.rpcOptions.getRpcDefaultTimeout() : timeoutMs);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            future.completeExceptionally(e);
            // should be in another thread to avoid dead locking.
            RpcUtils.runClosureInExecutor(currExecutor, done,
                new Status(RaftError.EINTR, "Sending rpc was interrupted"));
        } catch (final RemotingException e) {
            future.completeExceptionally(e);
            // should be in another thread to avoid dead locking.
            RpcUtils.runClosureInExecutor(currExecutor, done, new Status(RaftError.EINTERNAL,
                "Fail to send a RPC request:" + e.getMessage()));

        }

        return future;
    }

    private static Status handleErrorResponse(final ErrorResponse eResp) {
        final Status status = new Status();
        status.setCode(eResp.getErrorCode());
        status.setErrorMsg(eResp.getErrorMsg());
        return status;
    }

    private <T extends Message> void onCanceled(final Message request, final RpcResponseClosure<T> done) {
        if (done != null) {
            try {
                done.run(new Status(RaftError.ECANCELED, "RPC request was canceled by future."));
            } catch (final Throwable t) {
                LOG.log(System.Logger.Level.WARNING, "Fail to run RpcResponseClosure, the request is {}.", request, t);
            }
        }
    }

}
