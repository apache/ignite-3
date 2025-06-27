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

package org.apache.ignite.internal.cli.core.repl;

import static org.apache.ignite.internal.cli.commands.Options.Constants.JDBC_URL_KEY;
import static org.apache.ignite.internal.cli.commands.Options.Constants.JDBC_URL_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.JDBC_URL_OPTION_DESC;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.ignite.internal.cli.commands.cliconfig.TestConfigManagerProvider;
import org.apache.ignite.internal.cli.config.ConfigDefaultValueProvider;
import org.apache.ignite.internal.cli.event.Events;
import org.junit.jupiter.api.Test;
import picocli.CommandLine.Model.OptionSpec;

class SessionDefaultValueProviderTest {
    private static final String JDBC_URL_CONFIG = "jdbc:ignite:thin://127.0.0.1:10800";
    private static final String JDBC_URL_SESSION = "jdbcUrl";

    @Test
    void jdbcUrl() throws Exception {
        // Given
        Session session = new Session();
        ConfigDefaultValueProvider configDefaultValueProvider = new ConfigDefaultValueProvider(new TestConfigManagerProvider());
        SessionDefaultValueProvider provider = new SessionDefaultValueProvider(session, configDefaultValueProvider);

        OptionSpec spec = createJdbcUrlSpec();

        // When session is not connected
        // Then default value is taken from the config
        assertEquals(JDBC_URL_CONFIG, provider.defaultValue(spec));

        // When session is connected
        session.onEvent(Events.connect(SessionInfo.builder().nodeUrl("nodeUrl").nodeName("nodeName").jdbcUrl(JDBC_URL_SESSION).build()));

        // Then default value is taken from the session
        assertEquals(JDBC_URL_SESSION, provider.defaultValue(spec));
    }

    private static OptionSpec createJdbcUrlSpec() {
        return OptionSpec.builder(JDBC_URL_OPTION)
                .required(true)
                .descriptionKey(JDBC_URL_KEY)
                .description(JDBC_URL_OPTION_DESC)
                .build();
    }
}
