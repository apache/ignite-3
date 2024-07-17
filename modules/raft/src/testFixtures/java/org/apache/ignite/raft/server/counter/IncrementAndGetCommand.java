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

package org.apache.ignite.raft.server.counter;

import org.apache.ignite.internal.network.annotations.Transferable;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.raft.messages.TestMessageGroup;
import org.apache.ignite.raft.messages.TestRaftMessagesFactory;

/**
 * Increment and get command.
 */
// TODO IGNITE-18357 Move to integration test directory when Maven build is not supported anymore.
@Transferable(TestMessageGroup.INCREMENT_AND_GET_COMMAND)
public interface IncrementAndGetCommand extends WriteCommand {
    /**
     * Returns the delta.
     */
    long delta();

    static IncrementAndGetCommand incrementAndGetCommand(long delta) {
        return new TestRaftMessagesFactory().incrementAndGetCommand().delta(delta).build();
    }
}
