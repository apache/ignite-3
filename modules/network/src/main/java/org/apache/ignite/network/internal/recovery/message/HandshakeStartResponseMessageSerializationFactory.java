/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
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

package org.apache.ignite.network.internal.recovery.message;

import java.util.UUID;
import org.apache.ignite.network.internal.MessageReader;
import org.apache.ignite.network.message.MessageDeserializer;
import org.apache.ignite.network.message.MessageMappingException;
import org.apache.ignite.network.message.MessageSerializationFactory;
import org.apache.ignite.network.message.MessageSerializer;

/** */
public class HandshakeStartResponseMessageSerializationFactory implements MessageSerializationFactory<HandshakeStartResponseMessage> {
    /** {@inheritDoc} */
    @Override public MessageDeserializer<HandshakeStartResponseMessage> createDeserializer() {
        return new MessageDeserializer<HandshakeStartResponseMessage>() {
            /** */
            long connectCnt;

            /** */
            String consistentId;

            /** */
            UUID launchId;

            /** */
            long rcvCnt;

            /** */
            HandshakeStartResponseMessage message;

            /** {@inheritDoc} */
            @Override public boolean readMessage(MessageReader reader) throws MessageMappingException {
                if (!reader.beforeMessageRead())
                    return false;

                switch (reader.state()) {
                    case 0:
                        connectCnt = reader.readLong("connectCnt");

                        if (!reader.isLastRead())
                            return false;

                        reader.incrementState();

                        //noinspection fallthrough
                    case 1:
                        consistentId = reader.readString("consistentId");

                        if (!reader.isLastRead())
                            return false;

                        reader.incrementState();

                        //noinspection fallthrough
                    case 2:
                        launchId = reader.readUuid("launchId");

                        if (!reader.isLastRead())
                            return false;

                        reader.incrementState();

                        //noinspection fallthrough
                    case 3:
                        rcvCnt = reader.readLong("rcvCnt");

                        if (!reader.isLastRead())
                            return false;

                        reader.incrementState();

                }

                message = new HandshakeStartResponseMessage(launchId, consistentId, rcvCnt, connectCnt);

                return reader.afterMessageRead(HandshakeStartMessage.class);
            }

            /** {@inheritDoc} */
            @Override public Class<HandshakeStartResponseMessage> klass() {
                return HandshakeStartResponseMessage.class;
            }

            /** {@inheritDoc} */
            @Override public HandshakeStartResponseMessage getMessage() {
                return message;
            }
        };
    }

    /** {@inheritDoc} */
    @Override public MessageSerializer<HandshakeStartResponseMessage> createSerializer() {
        return (message, writer) -> {
            if (!writer.isHeaderWritten()) {
                if (!writer.writeHeader(message.directType(), (byte) 1))
                    return false;

                writer.onHeaderWritten();
            }

            switch (writer.state()) {
                case 0:
                    if (!writer.writeLong("connectCnt", message.connectCount()))
                        return false;

                    writer.incrementState();

                    //noinspection fallthrough
                case 1:
                    if (!writer.writeString("consistentId", message.consistentId()))
                        return false;

                    writer.incrementState();

                    //noinspection fallthrough
                case 2:
                    if (!writer.writeUuid("launchId", message.launchId()))
                        return false;

                    writer.incrementState();

                    //noinspection fallthrough
                case 3:
                    if (!writer.writeLong("rcvCnt", message.receivedCount()))
                        return false;

                    writer.incrementState();

            }

            return true;
        };
    }
}
