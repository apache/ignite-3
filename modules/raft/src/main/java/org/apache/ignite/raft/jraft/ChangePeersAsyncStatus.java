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

package org.apache.ignite.raft.jraft;

/**
 * Returning status of the {@link Node#changePeersAsync(org.apache.ignite.raft.jraft.conf.Configuration, long)} method invocation.
 */
public enum ChangePeersAsyncStatus {
    /**
     * Returned when the new peers passed to {@link Node#changePeersAsync} equal to the current configuration.
     */
    DONE,

    /**
     * Returned when the current term does not match with
     * the provided term in {@link Node#changePeersAsync(org.apache.ignite.raft.jraft.conf.Configuration, long)}
     */
    WRONG_TERM,

    /**
     * Returned when an asynchronous process of changing peers has been started.
     * That means that state of {@link org.apache.ignite.raft.jraft.core.NodeImpl.ConfigurationCtx} was switched to
     * {@code STAGE_CATCHING_UP}
     */
    RECEIVED,

    /**
     * Returned when there is a concurrent change peers process.
     */
    BUSY,

    /**
     * Returned when change peers async invocation failed.
     */
    FAILED
}
