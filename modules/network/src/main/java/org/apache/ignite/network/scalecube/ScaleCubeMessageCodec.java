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

package org.apache.ignite.network.scalecube;

import io.scalecube.cluster.transport.api.Message;
import io.scalecube.cluster.transport.api.MessageCodec;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.network.MessageMapper;
import org.apache.ignite.network.MessageMappingException;
import org.apache.ignite.network.NetworkMessage;

/**
 * Serializes and deserialized messages in ScaleCube cluster.
 */
class ScaleCubeMessageCodec implements MessageCodec {
    /** Header name for {@link NetworkMessage#type()}. */
    public static final String HEADER_MESSAGE_TYPE = "type";

    /** Map message type -> {@link MessageMapper} */
    private final Map<Short, MessageMapper> messageMapperMap;

    /**
     * Constructor.
     * @param map Message mapper map.
     */
    ScaleCubeMessageCodec(Map<Short, MessageMapper> map) {
        messageMapperMap = map;
    }

    /** {@inheritDoc} */
    @Override public Message deserialize(InputStream stream) throws Exception {
        Message.Builder builder = Message.builder();
        try (ObjectInputStream ois = new ObjectInputStream(stream)) {
            // headers
            int headersSize = ois.readInt();
            Map<String, String> headers = new HashMap<>(headersSize);
            for (int i = 0; i < headersSize; i++) {
                String name = ois.readUTF();
                String value = (String) ois.readObject();
                headers.put(name, value);
            }

            builder.headers(headers);

            String typeString = headers.get(HEADER_MESSAGE_TYPE);

            if (typeString == null) {
                builder.data(ois.readObject());
                return builder.build();
            }

            short type;
            try {
                type = Short.parseShort(typeString);
            }
            catch (NumberFormatException e) {
                throw new MessageMappingException("Type is not short", e);
            }

            MessageMapper mapper = messageMapperMap.get(type);

            assert mapper != null : "No mapper defined for type " + type;

            NetworkMessage message = mapper.readMessage(ois);
            builder.data(message);
        }
        return builder.build();
    }

    /** {@inheritDoc} */
    @Override public void serialize(Message message, OutputStream stream) throws Exception {
        final Object data = message.data();

        if (!(data instanceof NetworkMessage)) {
            try (ObjectOutputStream oos = new ObjectOutputStream(stream)) {
                message.writeExternal(oos);
            }
            return;
        }

        Map<String, String> headers = message.headers();

        assert headers.containsKey(HEADER_MESSAGE_TYPE) : "Missing message type header";

        try (ObjectOutputStream oos = new ObjectOutputStream(stream)) {
            // headers
            oos.writeInt(headers.size());
            for (Map.Entry<String, String> header : headers.entrySet()) {
                oos.writeUTF(header.getKey());
                oos.writeObject(header.getValue());
            }

            assert data instanceof NetworkMessage : "Message data is not an instance of NetworkMessage";

            NetworkMessage msg = (NetworkMessage) data;
            MessageMapper mapper = messageMapperMap.get(msg.type());

            assert mapper != null : "No mapper defined for type " + msg.getClass();

            mapper.writeMessage(msg, oos);

            oos.flush();
        }
    }
}
