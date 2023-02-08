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

package org.apache.ignite.internal.client;

import org.apache.ignite.client.KeystoreConfiguration;
import org.jetbrains.annotations.Nullable;

/** Implementation of client keystore configuration. */
public class KeystoreConfigurationImpl implements KeystoreConfiguration {
    private final String path;

    private final String password;

    private final String type;

    /** Main constructor. */
    KeystoreConfigurationImpl(@Nullable String path, @Nullable String password, String type) {
        this.path = path;
        this.password = password;
        this.type = type;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable String path() {
        return path;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable String password() {
        return password;
    }

    /** {@inheritDoc} */
    @Override
    public String type() {
        return type;
    }
}
