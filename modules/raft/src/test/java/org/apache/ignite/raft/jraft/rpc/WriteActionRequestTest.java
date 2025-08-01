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

package org.apache.ignite.raft.jraft.rpc;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.serialization.MessageSerializer;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.junit.jupiter.api.Test;

class WriteActionRequestTest {
    private static final RaftMessagesFactory MESSAGE_FACTORY = new RaftMessagesFactory();

    @Test
    void includesWriteActionCommandClassNameInToStringForLogging() {
        WriteActionRequest request = MESSAGE_FACTORY.writeActionRequest()
                .deserializedCommand(new TestWriteCommand())
                .command(new byte[0])
                .groupId("test")
                .build();

        assertThat(
                request.toStringForLightLogging(),
                is(WriteActionRequestImpl.class.getName() + "(" + TestWriteCommand.class.getName() + ")")
        );
    }

    @Test
    void toStringForLoggingSupportsNullCommands() {
        WriteActionRequest request = MESSAGE_FACTORY.writeActionRequest()
                .deserializedCommand(null)
                .command(new byte[0])
                .groupId("test")
                .build();

        assertThat(
                request.toStringForLightLogging(),
                is(WriteActionRequestImpl.class.getName() + "(null)")
        );
    }

    private static class TestWriteCommand implements WriteCommand {
        @Override
        public MessageSerializer<NetworkMessage> serializer() {
            throw new UnsupportedOperationException();
        }

        @Override
        public short messageType() {
            throw new UnsupportedOperationException();
        }

        @Override
        public short groupType() {
            throw new UnsupportedOperationException();
        }

        @Override
        public NetworkMessage clone() {
            throw new UnsupportedOperationException();
        }

        @Override
        public HybridTimestamp initiatorTime() {
            throw new UnsupportedOperationException();
        }
    }
}
