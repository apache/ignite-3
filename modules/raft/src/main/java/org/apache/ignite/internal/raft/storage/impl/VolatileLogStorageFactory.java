/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.raft.storage.impl;

import org.apache.ignite.internal.raft.storage.LogStorageFactory;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.storage.LogStorage;
import org.apache.ignite.raft.jraft.storage.impl.LocalLogStorage;

/**
 * Log storage factory based on {@link LocalLogStorage}.
 */
public class VolatileLogStorageFactory implements LogStorageFactory {
    /** {@inheritDoc} */
    @Override
    public void start() {
    }

    /** {@inheritDoc} */
    @Override
    public LogStorage getLogStorage(String groupId, RaftOptions raftOptions) {
        return new LocalLogStorage(raftOptions);
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws Exception {
    }
}
