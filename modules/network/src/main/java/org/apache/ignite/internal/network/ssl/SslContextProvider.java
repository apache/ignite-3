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

package org.apache.ignite.internal.network.ssl;

import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import org.apache.ignite.internal.network.configuration.SslView;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.apache.ignite.lang.IgniteException;

/** SSL context provider. */
public final class SslContextProvider {

    private SslContextProvider() {
    }

    /** Create client SSL context. */
    public static SslContext createClientSslContext(SslView ssl) {
        try {
            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init(KeystoreLoader.load(ssl.trustStore()));

            var builder = SslContextBuilder.forClient().trustManager(trustManagerFactory);

            ClientAuth clientAuth = ClientAuth.valueOf(ssl.clientAuth().toUpperCase());
            if (ClientAuth.NONE == clientAuth) {
                return builder.build();
            }

            KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keyManagerFactory.init(KeystoreLoader.load(ssl.keyStore()), ssl.keyStore().password().toCharArray());

            builder.keyManager(keyManagerFactory);

            return builder.build();
        } catch (NoSuchFileException e) {
            throw new IgniteException(Common.SSL_CONFIGURATION_ERR, String.format("File %s not found", e.getMessage()), e);
        } catch (IOException | CertificateException | KeyStoreException | NoSuchAlgorithmException | UnrecoverableKeyException e) {
            throw new IgniteException(Common.SSL_CONFIGURATION_ERR, e);
        }
    }

    /** Create server SSL context. */
    public static SslContext createServerSslContext(SslView ssl) {
        try {
            KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keyManagerFactory.init(KeystoreLoader.load(ssl.keyStore()), ssl.keyStore().password().toCharArray());

            var builder = SslContextBuilder.forServer(keyManagerFactory);

            ClientAuth clientAuth = ClientAuth.valueOf(ssl.clientAuth().toUpperCase());
            if (ClientAuth.NONE == clientAuth) {
                return builder.build();
            }

            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init(KeystoreLoader.load(ssl.trustStore()));

            builder.clientAuth(clientAuth).trustManager(trustManagerFactory);

            return builder.build();
        } catch (NoSuchFileException e) {
            throw new IgniteException(Common.SSL_CONFIGURATION_ERR, String.format("File %s not found", e.getMessage()), e);
        } catch (KeyStoreException | NoSuchAlgorithmException | CertificateException | UnrecoverableKeyException | IOException e) {
            throw new IgniteException(Common.SSL_CONFIGURATION_ERR, e);
        }
    }
}
