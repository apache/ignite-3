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

package org.apache.ignite.client.handler;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import io.netty.handler.ssl.util.SelfSignedCertificate;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import org.msgpack.core.MessagePack;

class ItClientHandlerTestUtils {
    /** Magic bytes. */
    static final byte[] MAGIC = {0x49, 0x47, 0x4E, 0x49};

    static void connectAndHandshake(ClientHandlerModule serverModule) throws IOException {
        connectAndHandshake(serverModule, false, false);
    }

    static void connectAndHandshake(ClientHandlerModule serverModule, boolean skipMagic, boolean badVersion) throws IOException {
        try (var sock = new Socket("127.0.0.1", serverModule.localAddress().getPort())) {
            OutputStream out = sock.getOutputStream();

            // Magic: IGNI
            if (!skipMagic) {
                out.write(MAGIC);
            }

            // Handshake.
            writeHandshake(badVersion, out);

            // Read response.
            checkResponseMagic(sock);
        }
    }

    static Socket connectAndHandshakeAndGetSocket(ClientHandlerModule serverModule) throws IOException {
        var sock = new Socket("127.0.0.1", serverModule.localAddress().getPort());
        OutputStream out = sock.getOutputStream();

        // Magic: IGNI
        out.write(MAGIC);

        // Handshake.
        writeHandshake(false, out);

        // Read response.
        checkResponseMagic(sock);

        return sock;
    }

    static String generateKeystore(Path workDir)
            throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException {
        var keyStorePkcs12Path = workDir.resolve("keystore.p12").toAbsolutePath().toString();
        var cert = new SelfSignedCertificate("localhost");

        KeyStore ks = KeyStore.getInstance("PKCS12");
        ks.load(null, null);
        ks.setKeyEntry("key", cert.key(), "changeit".toCharArray(), new Certificate[]{cert.cert()});

        try (FileOutputStream fos = new FileOutputStream(keyStorePkcs12Path)) {
            ks.store(fos, "changeit".toCharArray());
        }

        return keyStorePkcs12Path;
    }

    private static void writeHandshake(boolean badVersion, OutputStream out) throws IOException {
        try (var packer = MessagePack.newDefaultBufferPacker()) {
            packer.packInt(0);
            packer.packInt(0);
            packer.packInt(0);
            packer.packInt(7); // Size.

            packer.packInt(badVersion ? 42 : 3); // Major.
            packer.packInt(0); // Minor.
            packer.packInt(0); // Patch.

            packer.packInt(2); // Client type: general purpose.

            packer.packBinaryHeader(0); // Features.
            packer.packInt(0); // Extensions.

            out.write(packer.toByteArray());
            out.flush();
        }
    }

    private static void checkResponseMagic(Socket sock) throws IOException {
        var unpacker = MessagePack.newDefaultUnpacker(sock.getInputStream());
        var magic = unpacker.readPayload(4);

        assertArrayEquals(MAGIC, magic);
    }
}
