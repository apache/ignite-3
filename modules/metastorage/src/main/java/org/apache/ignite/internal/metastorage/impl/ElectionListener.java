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

package org.apache.ignite.internal.metastorage.impl;

import org.apache.ignite.network.ClusterNode;

/**
 * Describes an object which has interest in change of the leader.
 *
 * <p>Note: this is not part of meta storage api, but rather part of particular implementation. May be changed or removed with new
 * implementation of meta storage, if any. Use on your own risk.
 */
@FunctionalInterface
public interface ElectionListener {
    /** Notifies the listener that new leader has been elected. */
    void onLeaderElected(ClusterNode newLeader);
}
