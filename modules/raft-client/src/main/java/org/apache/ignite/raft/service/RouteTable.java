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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.StampedLock;
import org.apache.ignite.raft.Configuration;
import org.apache.ignite.raft.PeerId;
import org.apache.ignite.raft.RaftError;
import org.apache.ignite.raft.Status;
import org.apache.ignite.raft.rpc.CliRequests;
import org.apache.ignite.raft.rpc.Message;
import org.apache.ignite.raft.rpc.RpcRequests;

/**
 * Maintain routes to raft groups.
 */
public class RouteTable {
    private static final System.Logger LOG = System.getLogger(RouteTable.class.getName());

    // TODO fixme remove static table.
    private static final RouteTable INSTANCE = new RouteTable();

    // Map<groupId, groupConf>
    private final ConcurrentMap<String, GroupConf> groupConfTable = new ConcurrentHashMap<>();

    public static RouteTable getInstance() {
        return INSTANCE;
    }

    /**
     * Update configuration of group in route table.
     *
     * @param groupId raft group id
     * @param conf    configuration to update
     * @return true on success
     */
    public boolean updateConfiguration(final String groupId, final Configuration conf) {
        final GroupConf gc = getOrCreateGroupConf(groupId);
        final StampedLock stampedLock = gc.stampedLock;
        final long stamp = stampedLock.writeLock();
        try {
            gc.conf = conf;
            if (gc.leader != null && !gc.conf.contains(gc.leader)) {
                gc.leader = null;
            }
        } finally {
            stampedLock.unlockWrite(stamp);
        }
        return true;
    }

    private GroupConf getOrCreateGroupConf(final String groupId) {
        GroupConf gc = this.groupConfTable.get(groupId);
        if (gc == null) {
            gc = new GroupConf();
            final GroupConf old = this.groupConfTable.putIfAbsent(groupId, gc);
            if (old != null) {
                gc = old;
            }
        }
        return gc;
    }

    /**
     * Get the cached leader of the group, return it when found, null otherwise.
     * Make sure calls {@link #refreshLeader(RaftGroupClientService, String, int)} already
     * before invoke this method.
     *
     * @param groupId raft group id
     * @return peer of leader
     */
    public PeerId selectLeader(final String groupId) {
        final GroupConf gc = this.groupConfTable.get(groupId);
        if (gc == null) {
            return null;
        }
        final StampedLock stampedLock = gc.stampedLock;
        long stamp = stampedLock.tryOptimisticRead();
        PeerId leader = gc.leader;
        if (!stampedLock.validate(stamp)) {
            stamp = stampedLock.readLock();
            try {
                leader = gc.leader;
            } finally {
                stampedLock.unlockRead(stamp);
            }
        }
        return leader;
    }

    /**
     * Update leader info.
     *
     * @param groupId raft group id
     * @param leader  peer of leader
     * @return true on success
     */
    public boolean updateLeader(final String groupId, final PeerId leader) {
        final GroupConf gc = getOrCreateGroupConf(groupId);
        final StampedLock stampedLock = gc.stampedLock;
        final long stamp = stampedLock.writeLock();
        try {
            gc.leader = leader;
        } finally {
            stampedLock.unlockWrite(stamp);
        }
        return true;
    }

    /**
     * Update leader info.
     *
     * @param groupId   raft group id
     * @param leaderStr peer string of leader
     * @return true on success
     */
    public boolean updateLeader(final String groupId, final String leaderStr) {
        final PeerId leader = new PeerId();
        if (leader.parse(leaderStr)) {
            return updateLeader(groupId, leader);
        } else {
            return false;
        }
    }

    /**
     * Get the configuration by groupId, returns null when not found.
     *
     * @param groupId raft group id
     * @return configuration of the group id
     */
    public Configuration getConfiguration(final String groupId) {
        final GroupConf gc = this.groupConfTable.get(groupId);
        if (gc == null) {
            return null;
        }
        final StampedLock stampedLock = gc.stampedLock;
        long stamp = stampedLock.tryOptimisticRead();
        Configuration conf = gc.conf;
        if (!stampedLock.validate(stamp)) {
            stamp = stampedLock.readLock();
            try {
                conf = gc.conf;
            } finally {
                stampedLock.unlockRead(stamp);
            }
        }
        return conf;
    }

    /**
     * Blocking the thread until query_leader finishes.
     *
     * @param groupId   raft group id
     * @param timeoutMs timeout millis
     * @return operation status
     */
    public Status refreshLeader(final RaftGroupClientService raftGroupClientService, final String groupId, final int timeoutMs)
        throws InterruptedException,
        TimeoutException {
        final Configuration conf = getConfiguration(groupId);
        if (conf == null) {
            return new Status(RaftError.ENOENT,
                "Group %s is not registered in RouteTable, forgot to call updateConfiguration?", groupId);
        }
        final Status st = Status.OK();
        final CliRequests.GetLeaderRequest.Builder rb = CliRequests.GetLeaderRequest.newBuilder();
        rb.setGroupId(groupId);
        final CliRequests.GetLeaderRequest request = rb.build();
        TimeoutException timeoutException = null;
        for (final PeerId peer : conf) {
            if (!raftGroupClientService.connect(peer.getEndpoint())) {
                if (st.isOk()) {
                    st.setError(-1, "Fail to init channel to %s", peer);
                } else {
                    final String savedMsg = st.getErrorMsg();
                    st.setError(-1, "%s, Fail to init channel to %s", savedMsg, peer);
                }
                continue;
            }
            final Future<Message> result = raftGroupClientService.getLeader(peer.getEndpoint(), request, null);
            try {
                final Message msg = result.get(timeoutMs, TimeUnit.MILLISECONDS);
                if (msg instanceof RpcRequests.ErrorResponse) {
                    if (st.isOk()) {
                        st.setError(-1, ((RpcRequests.ErrorResponse) msg).getErrorMsg());
                    } else {
                        final String savedMsg = st.getErrorMsg();
                        st.setError(-1, "%s, %s", savedMsg, ((RpcRequests.ErrorResponse) msg).getErrorMsg());
                    }
                } else {
                    final CliRequests.GetLeaderResponse response = (CliRequests.GetLeaderResponse) msg;
                    updateLeader(groupId, response.getLeaderId());
                    return Status.OK();
                }
            } catch (final TimeoutException e) {
                timeoutException = e;
            } catch (final ExecutionException e) {
                if (st.isOk()) {
                    st.setError(-1, e.getMessage());
                } else {
                    final String savedMsg = st.getErrorMsg();
                    st.setError(-1, "%s, %s", savedMsg, e.getMessage());
                }
            }
        }
        if (timeoutException != null) {
            throw timeoutException;
        }

        return st;
    }

    public Status refreshConfiguration(final RaftGroupClientService raftGroupClientService, final String groupId,
                                       final int timeoutMs) throws InterruptedException, TimeoutException {
        final Configuration conf = getConfiguration(groupId);
        if (conf == null) {
            return new Status(RaftError.ENOENT,
                "Group %s is not registered in RouteTable, forgot to call updateConfiguration?", groupId);
        }
        final Status st = Status.OK();
        PeerId leaderId = selectLeader(groupId);
        if (leaderId == null) {
            refreshLeader(raftGroupClientService, groupId, timeoutMs);
            leaderId = selectLeader(groupId);
        }
        if (leaderId == null) {
            st.setError(-1, "Fail to get leader of group %s", groupId);
            return st;
        }
        if (!raftGroupClientService.connect(leaderId.getEndpoint())) {
            st.setError(-1, "Fail to init channel to %s", leaderId);
            return st;
        }
        final CliRequests.GetPeersRequest.Builder rb = CliRequests.GetPeersRequest.newBuilder();
        rb.setGroupId(groupId);
        rb.setLeaderId(leaderId.toString());
        try {
            final Message result = raftGroupClientService.getPeers(leaderId.getEndpoint(), rb.build(), null).get(timeoutMs,
                TimeUnit.MILLISECONDS);
            if (result instanceof CliRequests.GetPeersResponse) {
                final CliRequests.GetPeersResponse resp = (CliRequests.GetPeersResponse) result;
                final Configuration newConf = new Configuration();
                for (final String peerIdStr : resp.getPeersList()) {
                    final PeerId newPeer = new PeerId();
                    newPeer.parse(peerIdStr);
                    newConf.addPeer(newPeer);
                }
                updateConfiguration(groupId, newConf);
            } else {
                final RpcRequests.ErrorResponse resp = (RpcRequests.ErrorResponse) result;
                st.setError(resp.getErrorCode(), resp.getErrorMsg());
            }
        } catch (final Exception e) {
            st.setError(-1, e.getMessage());
        }
        return st;
    }

    /**
     * Reset the states.
     */
    public void reset() {
        this.groupConfTable.clear();
    }

    /**
     * Remove the group from route table.
     *
     * @param groupId raft group id
     * @return true on success
     */
    public boolean removeGroup(final String groupId) {
        return this.groupConfTable.remove(groupId) != null;
    }

    @Override
    public String toString() {
        return "RouteTable{" + "groupConfTable=" + groupConfTable + '}';
    }

    private RouteTable() {
    }

    private static class GroupConf {

        private final StampedLock stampedLock = new StampedLock();

        private Configuration conf;
        private PeerId leader;

        @Override
        public String toString() {
            return "GroupConf{" + "conf=" + conf + ", leader=" + leader + '}';
        }
    }
}
