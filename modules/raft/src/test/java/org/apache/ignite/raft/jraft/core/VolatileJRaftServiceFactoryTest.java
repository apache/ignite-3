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

package org.apache.ignite.raft.jraft.core;

import org.apache.ignite.hlc.HybridClock;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.storage.VolatileStorage;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

class VolatileJRaftServiceFactoryTest {
    private final VolatileJRaftServiceFactory serviceFactory = new VolatileJRaftServiceFactory(new HybridClock());

    @Test
    void producesVolatileMetaStorage() {
        assertThat(serviceFactory.createRaftMetaStorage("test", new RaftOptions()), is(instanceOf(VolatileStorage.class)));
    }

    @Test
    void producesVolatileLogStorage() {
        assertThat(serviceFactory.createLogStorage("test", new RaftOptions()), is(instanceOf(VolatileStorage.class)));
    }
}