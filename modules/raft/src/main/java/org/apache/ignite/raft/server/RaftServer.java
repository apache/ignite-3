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

package org.apache.ignite.raft.server;

import org.apache.ignite.network.NetworkMember;
import org.apache.ignite.raft.client.service.RaftGroupCommandListener;

/**
 * The RAFT replication group server. Provides server side RAFT protocol capabilities.
 * <p>
 * The raft group listens for client commands, submits them to a replicatedlog and calls {@link RaftGroupCommandListener}
 * methods then a command's result is commited to the log.
 */
public interface RaftServer extends Lifecycle<RaftServerOptions> {
    NetworkMember networkMember();

    void setListener(String groupId, RaftGroupCommandListener lsnr);

    void clearListener(String groupId);

    @Override void init(RaftServerOptions options);

    @Override void destroy() throws Exception;
}
