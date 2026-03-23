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
package org.apache.ignite.raft.jraft;

import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.raft.jraft.core.NodeImpl;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.rpc.RpcServer;
import org.apache.ignite.raft.jraft.util.StringUtils;

/**
 * A raft group service.
 */
public class RaftGroupService {
    private static final IgniteLogger LOG = Loggers.forClass(RaftGroupService.class);

    private volatile boolean started = false;

    /**
     * This node serverId
     */
    private PeerId serverId;

    /**
     * Node options
     */
    private NodeOptions nodeOptions;

    /**
     * The raft RPC server
     */
    private RpcServer rpcServer;

    /**
     * The raft group id
     */
    private String groupId;

    /**
     * The raft node.
     */
    private NodeImpl node;

    /**
     * The node manager.
     */
    private NodeManager nodeManager;

    /**
     * @param groupId Group Id.
     * @param serverId Server id.
     * @param nodeOptions Node options.
     * @param rpcServer RPC server.
     */
    public RaftGroupService(
            final String groupId,
            final PeerId serverId,
            final NodeOptions nodeOptions,
            final RpcServer rpcServer
    ) {
        this.groupId = groupId;
        this.serverId = serverId;
        this.nodeOptions = nodeOptions;
        this.rpcServer = rpcServer;
        this.nodeManager = nodeOptions.getNodeManager();
    }

    public synchronized Node getRaftNode() {
        return this.node;
    }

    /**
     * Starts the raft group service, returns the raft node.
     */
    public synchronized Node start() {
        if (this.started) {
            return this.node;
        }
        if (this.serverId == null || this.serverId.isEmpty()) {
            throw new IllegalArgumentException("Blank serverId:" + this.serverId);
        }
        if (StringUtils.isBlank(this.groupId)) {
            throw new IllegalArgumentException("Blank group id" + this.groupId);
        }

        assert this.nodeOptions.getRpcClient() != null;

        this.node = new NodeImpl(groupId, serverId);

        if (!this.node.init(this.nodeOptions)) {
            LOG.warn("Stopping partially started node [groupId={}, serverId={}]", groupId, serverId);
            this.node.shutdown();

            try {
                this.node.join();
            }
            catch (InterruptedException e) {
                throw new IgniteInternalException(e);
            }

            throw new IgniteInternalException("Fail to init node, please see the logs to find the reason.");
        }

        this.nodeManager.add(this.node);
        this.started = true;
        LOG.info("Start the RaftGroupService successfully [node={}].", this.node.getNodeId());
        return this.node;
    }

    public synchronized void shutdown() {
        // TODO asch remove handlers before shutting down raft node https://issues.apache.org/jira/browse/IGNITE-14519
        if (!this.started) {
            return;
        }

        this.node.shutdown();
        try {
            this.node.join();
        }
        catch (InterruptedException e) {
            LOG.error("Interrupted while waiting for the node to shutdown [node={}].", this.node.getNodeId());
        }

        nodeManager.remove(this.node);
        this.started = false;
        LOG.info("Stop the RaftGroupService successfully [node={}].", this.node.getNodeId());
    }

    /**
     * Returns true when service is started.
     */
    public boolean isStarted() {
        return this.started;
    }

    /**
     * Returns the raft group id.
     */
    public String getGroupId() {
        return this.groupId;
    }

    /**
     * Set the raft group id
     */
    public void setGroupId(final String groupId) {
        if (this.started) {
            throw new IllegalStateException("Raft group service already started");
        }
        this.groupId = groupId;
    }

    /**
     * Returns the node serverId
     */
    public PeerId getServerId() {
        return this.serverId;
    }

    /**
     * Set the node serverId
     */
    public void setServerId(final PeerId serverId) {
        if (this.started) {
            throw new IllegalStateException("Raft group service already started");
        }
        this.serverId = serverId;
    }

    /**
     * Returns the node options.
     */
    public NodeOptions getNodeOptions() {
        return this.nodeOptions;
    }

    /**
     * Set node options.
     */
    public void setNodeOptions(final NodeOptions nodeOptions) {
        if (this.started) {
            throw new IllegalStateException("Raft group service already started");
        }
        if (nodeOptions == null) {
            throw new IllegalArgumentException("Invalid node options.");
        }
        nodeOptions.validate();
        this.nodeOptions = nodeOptions;
    }

    /**
     * Returns the rpc server instance.
     */
    public RpcServer getRpcServer() {
        return this.rpcServer;
    }

    /**
     * Set rpc server.
     */
    public void setRpcServer(final RpcServer rpcServer) {
        if (this.started) {
            throw new IllegalStateException("Raft group service already started");
        }
        if (this.serverId == null) {
            throw new IllegalStateException("Please set serverId at first");
        }
        this.rpcServer = rpcServer;
    }
}
