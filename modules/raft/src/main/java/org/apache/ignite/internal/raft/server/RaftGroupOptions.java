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

package org.apache.ignite.internal.raft.server;

/**
 * Options specific to a Raft group that is being started.
 */
public class RaftGroupOptions {
    private final boolean volatileStores;

    public static RaftGroupOptions defaults() {
        return forPersistentStores();
    }

    public static RaftGroupOptions forPersistentStores() {
        return new RaftGroupOptions(false);
    }

    public static RaftGroupOptions forVolatileStores() {
        return new RaftGroupOptions(true);
    }

    public RaftGroupOptions(boolean volatileStores) {
        this.volatileStores = volatileStores;
    }

    /**
     * Returns {@code true} if the Raft group should store its metadata and logs in volatile storages, or {@code false}
     * if these storages should be persistent.
     *
     * @return {@code true} if the Raft group should store its metadata and logs in volatile storages, or {@code false}
     *     if these storages should be persistent.
     */
    public boolean volatileStores() {
        return volatileStores;
    }
}
