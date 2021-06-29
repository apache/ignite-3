//
// MessagePack for Java
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//        http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//

package org.apache.ignite.clientconnector;

import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.buffer.ChannelBufferOutput;
import org.msgpack.core.buffer.MessageBufferOutput;
import org.msgpack.core.buffer.OutputStreamBufferOutput;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.UUID;

/**
 * Ignite-specific MsgPack extension.
 */
class ClientMessagePacker extends MessagePacker {

    /**
     * Create an MessagePacker that outputs the packed data to the given {@link MessageBufferOutput}.
     * This method is available for subclasses to override.
     * Use MessagePack.PackerConfig.newPacker method to instantiate this implementation.
     *
     * @param out    MessageBufferOutput. Use {@link OutputStreamBufferOutput}, {@link ChannelBufferOutput} or
     *               your own implementation of {@link MessageBufferOutput} interface.
     * @param config Config.
     */
    public ClientMessagePacker(MessageBufferOutput out, MessagePack.PackerConfig config) {
        super(out, config);
    }

    ClientMessagePacker packUuid(UUID v) throws IOException {
        packExtensionTypeHeader(ClientMsgPackType.UUID, 16);

        var bytes = new byte[16];
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        bb.order(ByteOrder.BIG_ENDIAN);

        bb.putLong(v.getMostSignificantBits());
        bb.putLong(v.getLeastSignificantBits());

        writePayload(bytes);

        return this;
    }
}
