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

package org.apache.ignite.internal.network.configuration;

/** Utility class for configuration tests. */
class TestUtils {

    /** Create a stub for {@link KeyStoreView}. */
    static KeyStoreView stubKeyStoreView(String type, String path, String password) {
        return new KeyStoreView() {

            @Override
            public String type() {
                return type;
            }

            @Override
            public String path() {
                return path;
            }

            @Override
            public String password() {
                return password;
            }
        };
    }

    /** Creates a stub for {@link SslView}. */
    static SslView stubSslView(boolean enabled, KeyStoreView keyStore, KeyStoreView trustStore) {
        return new SslView() {
            @Override
            public boolean enabled() {
                return enabled;
            }

            @Override
            public KeyStoreView keyStore() {
                return keyStore;
            }

            @Override
            public KeyStoreView trustStore() {
                return trustStore;
            }
        };
    }
}
