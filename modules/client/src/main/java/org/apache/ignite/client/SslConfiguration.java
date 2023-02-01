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

package org.apache.ignite.client;

import org.apache.ignite.internal.client.SslConfigurationImpl;
import org.jetbrains.annotations.Nullable;

/** Client ssl configuration. */
public interface SslConfiguration {

    /** If set to {@code true} then the ssl connection will be used to interact with Ignite 3 node. */
    boolean enabled();

    /** Truststore path that will be used to setup the ssl connection. */
    @Nullable String trustStorePath();

    /** Truststore password that will be used to setup the ssl connection. */
    @Nullable String trustStorePassword();

    /** Truststore type that will be used to setup the ssl connection. */
    @Nullable String trustStoreType();

    /** Create {@link SslConfiguration} from truststore type, path, and password. */
    static SslConfiguration trustStore(String trustStoreType, String trustStorePath, String trustStorePassword) {
        return new SslConfigurationImpl(true, trustStoreType, trustStorePath, trustStorePassword);
    }

    /** Create {@link SslConfiguration} from trust store path and password. */
    static SslConfiguration trustStore(String trustStorePath, String trustStorePassword) {
        return new SslConfigurationImpl(true, "PKCS12", trustStorePath, trustStorePassword);
    }

    /** Create {@link SslConfiguration} from trust store path. */
    static SslConfiguration trustStore(String trustStorePath) {
        return new SslConfigurationImpl(true, "PKCS12", trustStorePath, null);
    }

    /** Create {@link SslConfiguration} that is disabled (no ssl). */
    static SslConfiguration disabled() {
        return new SslConfigurationImpl(false, null, null, null);
    }
}
