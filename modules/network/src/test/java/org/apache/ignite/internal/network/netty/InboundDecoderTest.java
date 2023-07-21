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

package org.apache.ignite.internal.network.netty;

import static java.nio.file.Files.readAllBytes;
import static org.apache.ignite.utils.ClusterServiceTestUtils.defaultSerializationRegistry;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.apache.ignite.internal.network.AllTypesMessageGenerator;
import org.apache.ignite.internal.network.direct.DirectMessageWriter;
import org.apache.ignite.internal.network.messages.AllTypesMessage;
import org.apache.ignite.internal.network.messages.FileListMessage;
import org.apache.ignite.internal.network.messages.FileMessage;
import org.apache.ignite.internal.network.messages.NestedMessageMessage;
import org.apache.ignite.internal.network.messages.TestMessage;
import org.apache.ignite.internal.network.messages.TestMessagesFactory;
import org.apache.ignite.internal.network.serialization.PerSessionSerializationService;
import org.apache.ignite.internal.network.serialization.SerializationService;
import org.apache.ignite.internal.network.serialization.UserObjectSerializationContext;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.network.serialization.MessageSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

/**
 * Tests for {@link InboundDecoder}.
 */
public class InboundDecoderTest {
    /** {@link ByteBuf} allocator. */
    private final UnpooledByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;

    /** Registry. */
    private final MessageSerializationRegistry registry = defaultSerializationRegistry();

    private final TestMessagesFactory factory = new TestMessagesFactory();

    /**
     * Tests that an {@link InboundDecoder} can successfully read a message with all types supported by direct marshalling.
     *
     * @param seed Random seed.
     */
    @ParameterizedTest
    @MethodSource("messageGenerationSeed")
    public void testAllTypes(long seed) {
        AllTypesMessage msg = AllTypesMessageGenerator.generate(seed, true);

        AllTypesMessage received = sendAndReceive(msg);

        assertEquals(msg, received);
    }

    @ParameterizedTest
    @MethodSource("testFiles")
    public void testFile(File file) {
        // given
        FileMessage msg = factory.fileMessage().file(file).build();
        FileMessage received = sendAndReceive(msg);

        // then
        assertContentEquals(file.toPath(), received.file().toPath());
    }

    @Test
    public void nullFile() {
        // given
        FileMessage msg = factory.fileMessage().file(null).build();

        // then
        FileMessage received = sendAndReceive(msg);
        assertThat(received.file(), is(nullValue()));
    }

    @Test
    public void emptyFile() {
        // given
        File emptyFile = randomFile(0);
        FileMessage msg = factory.fileMessage().file(emptyFile).build();

        // then
        FileMessage received = sendAndReceive(msg);
        assertContentEquals(emptyFile.toPath(), received.file().toPath());
    }

    @Test
    public void testBigFileWithSmallBuffer() {
        // given
        File file = randomFile(1024);
        FileMessage msg = factory.fileMessage().file(file).build();

        // then
        FileMessage received = sendAndReceive(msg, 20);
        assertContentEquals(file.toPath(), received.file().toPath());
    }

    @Test
    public void testFilesList() throws IOException {
        // given
        List<File> files = testFiles().collect(Collectors.toList());
        FileListMessage msg = factory.fileListMessage().files(files).build();

        // then
        FileListMessage received = sendAndReceive(msg);
        assertThat(received.files(), hasSize(files.size()));
        for (int i = 0; i < files.size(); i++) {
            assertContentEquals(files.get(i).toPath(), received.files().get(i).toPath());
        }
    }

    @Test
    public void testBigFilesListWithSmallBuffer() {
        // given
        List<File> files = IntStream.range(0, 5)
                .mapToObj(i -> randomFile(1024))
                .collect(Collectors.toList());
        FileListMessage msg = factory.fileListMessage().files(files).build();

        // then
        FileListMessage received = sendAndReceive(msg, 20);
        assertThat(received.files(), hasSize(files.size()));
        for (int i = 0; i < files.size(); i++) {
            assertContentEquals(files.get(i).toPath(), received.files().get(i).toPath());
        }
    }

    @Test
    public void testNonExistingFile() {
        // given
        File file = new File("non-existing-file");
        FileMessage msg = factory.fileMessage().file(file).build();

        // then
        Exception exception = assertThrows(Exception.class, () -> sendAndReceive(msg));
        assertThat(exception.getCause(), instanceOf(NoSuchFileException.class));
    }

    @Test
    public void testFileWithoutReadPermissions() {
        // given
        File file = randomFile(10);
        assertTrue(file.setReadable(false));
        FileMessage msg = factory.fileMessage().file(file).build();

        // then
        Exception exception = assertThrows(Exception.class, () -> sendAndReceive(msg));
        assertThat(exception.getCause(), instanceOf(AccessDeniedException.class));
    }

    /**
     * Tests that the {@link InboundDecoder} is able to transfer a message with a nested {@code null} message (happy case is tested by
     * {@link #testAllTypes}).
     */
    @Test
    public void testNullNestedMessage() {
        NestedMessageMessage msg = new TestMessagesFactory().nestedMessageMessage()
                .nestedMessage(null)
                .build();

        NestedMessageMessage received = sendAndReceive(msg);

        assertNull(received.nestedMessage());
    }

    /**
     * Serializes and then deserializes the given message.
     */
    private <T extends NetworkMessage> T sendAndReceive(T msg) {
        return sendAndReceive(msg, 10_000);
    }

    private <T extends NetworkMessage> T sendAndReceive(T msg, int bufferSize) {
        var serializationService = new SerializationService(registry, mock(UserObjectSerializationContext.class));
        var perSessionSerializationService = new PerSessionSerializationService(serializationService);
        var channel = new EmbeddedChannel(new InboundDecoder(perSessionSerializationService));

        var writer = new DirectMessageWriter(registry, ConnectionManager.DIRECT_PROTOCOL_VERSION);

        MessageSerializer<NetworkMessage> serializer = registry.createSerializer(msg.groupType(), msg.messageType());

        ByteBuffer buf = ByteBuffer.allocate(bufferSize);

        T received;

        do {
            buf.clear();

            writer.setBuffer(buf);

            serializer.writeMessage(msg, writer);

            buf.flip();

            ByteBuf buffer = allocator.buffer(buf.limit());

            buffer.writeBytes(buf);

            channel.writeInbound(buffer);
        } while ((received = channel.readInbound()) == null);

        assertFalse(channel.finish());

        return received;
    }

    /**
     * Tests that an {@link InboundDecoder} doesn't hang if it encounters a byte buffer with only partially written header.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPartialHeader() throws Exception {
        var serializationService = new SerializationService(registry, mock(UserObjectSerializationContext.class));
        var perSessionSerializationService = new PerSessionSerializationService(serializationService);
        var channel = new EmbeddedChannel(new InboundDecoder(perSessionSerializationService));

        ByteBuf buffer = allocator.buffer();

        buffer.writeByte(1);

        CompletableFuture
                .runAsync(() -> {
                    channel.writeInbound(buffer);

                    // In case if a header was written partially, readInbound() should not loop forever, as
                    // there might not be new data anymore (example: remote host gone offline).
                    channel.readInbound();
                })
                .get(3, TimeUnit.SECONDS);

        assertFalse(channel.finish());
    }

    /**
     * Tests that an {@link InboundDecoder} can handle a {@link ByteBuf} where reader index is not {@code 0} at the start of the
     * {@link InboundDecoder#decode}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPartialReadWithReuseBuffer() throws Exception {
        ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);

        var channel = new EmbeddedChannel();

        Mockito.doReturn(channel).when(ctx).channel();

        var serializationService = new SerializationService(registry, mock(UserObjectSerializationContext.class));
        var perSessionSerializationService = new PerSessionSerializationService(serializationService);
        final var decoder = new InboundDecoder(perSessionSerializationService);

        final var list = new ArrayList<>();

        var writer = new DirectMessageWriter(registry, ConnectionManager.DIRECT_PROTOCOL_VERSION);

        var msg = new TestMessagesFactory().testMessage().msg("abcdefghijklmn").build();

        MessageSerializer<NetworkMessage> serializer = registry.createSerializer(msg.groupType(), msg.messageType());

        ByteBuffer nioBuffer = ByteBuffer.allocate(10_000);

        writer.setBuffer(nioBuffer);

        // Write message to the ByteBuffer.
        boolean fullyWritten = serializer.writeMessage(msg, writer);

        assertTrue(fullyWritten);

        nioBuffer.flip();

        ByteBuf buffer = allocator.buffer();

        // Write first 3 bytes of a message.
        for (int i = 0; i < 3; i++) {
            buffer.writeByte(nioBuffer.get());
        }

        decoder.decode(ctx, buffer, list);

        // At this point a header and a first byte of a message have been decoded.
        assertEquals(0, list.size());

        // Write next 3 bytes of a message.
        for (int i = 0; i < 3; i++) {
            buffer.writeByte(nioBuffer.get());
        }

        // Reader index of a buffer is not zero and it must be handled correctly by the InboundDecoder.
        decoder.decode(ctx, buffer, list);

        // Check if reader index has been tracked correctly.
        assertEquals(6, buffer.readerIndex());

        assertEquals(0, list.size());

        buffer.writeBytes(nioBuffer);

        decoder.decode(ctx, buffer, list);

        buffer.release();

        assertEquals(1, list.size());

        TestMessage actualMessage = (TestMessage) list.get(0);

        assertEquals(msg, actualMessage);

        assertFalse(channel.finish());
    }

    private static void assertContentEquals(Path expected, Path actual) {
        try {
            assertThat(readAllBytes(expected), is(readAllBytes(actual)));
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Source of parameters for the {@link #testAllTypes(long)} method. Creates seeds for a {@link AllTypesMessage} generation.
     *
     * @return Random seeds.
     */
    private static LongStream messageGenerationSeed() {
        var random = new Random();
        return IntStream.range(0, 100).mapToLong(ignored -> random.nextLong());
    }

    private static Stream<File> testFiles() throws IOException {
        URL resource = InboundDecoderTest.class.getClassLoader().getResource("files");
        return Files.list(Paths.get(resource.getPath()))
                .map(it -> it.toFile());
    }

    private static File randomFile(int contentSize) {
        try {
            Path tempFile = Files.createTempFile(null, null);
            byte[] bytes = new byte[contentSize];
            new Random().nextBytes(bytes);
            return Files.write(tempFile, bytes).toFile();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
