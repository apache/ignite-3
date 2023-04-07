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

package org.apache.ignite.internal.cli.config;

import org.apache.ignite.internal.cli.commands.cliconfig.TestConfigManagerHelper;

/**
 * Test factory for application state config.
 */
public class TestStateConfigHelper {
    private static final String EMPTY = "empty.ini";
    private static final String LAST_CONNECTED_DEFAULT = "last_connected_default.ini";
    private static final String LAST_CONNECTED_SSL_DEFAULT = "last_connected_ssl_default.ini";

    public static Config createEmptyConfig() {
        return createConfig(EMPTY);
    }

    public static Config createLastConnectedDefault() {
        return createConfig(LAST_CONNECTED_DEFAULT);
    }

    public static Config createLastConnectedSslDefault() {
        return createConfig(LAST_CONNECTED_SSL_DEFAULT);
    }

    private static Config createConfig(String resource) {
        return StateConfig.getStateConfig(TestConfigManagerHelper.copyResourceToTempFile(resource));
    }
}
