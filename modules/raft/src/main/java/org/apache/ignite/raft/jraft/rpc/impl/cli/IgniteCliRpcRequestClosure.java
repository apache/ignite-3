package org.apache.ignite.raft.jraft.rpc.impl.cli;

import org.apache.ignite.raft.jraft.Closure;
import org.apache.ignite.raft.jraft.Node;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.rpc.RaftRpcFactory;
import org.apache.ignite.raft.jraft.rpc.RpcContext;
import org.apache.ignite.raft.jraft.rpc.RpcRequestClosure;
import org.apache.ignite.raft.jraft.rpc.RpcRequests;

public class IgniteCliRpcRequestClosure implements Closure {
    private final Node node;
    private final RpcRequestClosure delegate;

    IgniteCliRpcRequestClosure(Node node, RpcRequestClosure closure) {
        this.node = node;
        this.delegate = closure;
    }

    public RpcContext getRpcCtx() {
        return delegate.getRpcCtx();
    }

    public RaftMessagesFactory getMsgFactory() {
        return delegate.getMsgFactory();
    }

    public void sendResponse(Message msg) {
        if (msg instanceof RpcRequests.ErrorResponse) {
            var err = (RpcRequests.ErrorResponse)msg;

            PeerId newLeader;

            if (err.errorCode() == RaftError.EPERM.getNumber()) {
                newLeader = node.getLeaderId();
                delegate.sendResponse(
                    RaftRpcFactory.DEFAULT
                        .newResponse(newLeader.toString(), getMsgFactory(), err.errorCode(), err.errorMsg()));
            }
            else
                delegate.sendResponse(msg);

        }
        else
            delegate.sendResponse(msg);
    }

    @Override public void run(Status status) {
        sendResponse(RaftRpcFactory.DEFAULT.newResponse(getMsgFactory(), status));
    }
}
