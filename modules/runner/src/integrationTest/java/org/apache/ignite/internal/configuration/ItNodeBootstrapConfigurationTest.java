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

package org.apache.ignite.internal.configuration;

import static org.apache.ignite.internal.ConfigTemplates.NL;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteServer;
import org.apache.ignite.InitParameters;
import org.apache.ignite.configuration.validation.ConfigurationValidationException;
import org.apache.ignite.internal.TestWrappers;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.configuration.presentation.HoconPresentation;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.lang.IgniteException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Integration tests for the node bootstrap configuration.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class ItNodeBootstrapConfigurationTest {
    @WorkDirectory
    private Path workDir;

    @Test
    public void wrongConfigurationFormat(TestInfo testInfo) {
        String config = UUID.randomUUID().toString();

        IgniteException igniteException = assertThrows(
                IgniteException.class,
                () -> TestIgnitionManager.start(testNodeName(testInfo, 0), config, workDir)
        );
        assertThat(igniteException, is(instanceOf(NodeConfigParseException.class)));
        assertThat(
                igniteException.getMessage(),
                containsString("Failed to parse config content from file " + workDir.resolve(TestIgnitionManager.DEFAULT_CONFIG_NAME))
        );
    }

    @Test
    public void illegalConfigurationValue(TestInfo testInfo) {
        String config = "ignite {" + NL
                + "  rest: {" + NL
                + "    ssl: {" + NL
                + "      enabled: true," + NL
                + "      clientAuth: none," + NL
                + "      keyStore: {" + NL
                + "        path: bad_path" + NL
                + "      }" + NL
                + "    }" + NL
                + "  }" + NL
                + "}";

        assertThrowsWithCause(
                () -> TestIgnitionManager.start(testNodeName(testInfo, 0), config, workDir),
                ConfigurationValidationException.class,
                "Validation did not pass for keys: [ignite.rest.ssl.keyStore, Key store file doesn't exist at bad_path]");
    }

    @Test
    public void testConfigurationValidationFailsWithDuplicates(TestInfo testInfo) {
        String config = "ignite {" + NL
                + "  rest: {" + NL
                + "    ssl: {" + NL
                + "      enabled: false," + NL
                + "      enabled: true" + NL
                + "    }" + NL
                + "  }" + NL
                + "}";

        assertThrowsWithCause(
                () -> TestIgnitionManager.start(testNodeName(testInfo, 0), config, workDir),
                ConfigurationValidationException.class,
                "Validation did not pass for keys: [ignite.rest.ssl.enabled, Duplicated key]");
    }

    @Test
    public void configurationFileNotModifiedAfterStart(TestInfo testInfo) throws IOException {
        String config = "ignite {" + NL
                + "  rest: {" + NL
                + "    port: 10300" + NL
                + "    ssl: {" + NL
                + "      enabled: false" + NL
                + "    }" + NL
                + "  }" + NL
                + "  network: {" + NL
                + "    port: 3344" + NL
                + "    nodeFinder {" + NL
                + "      netClusterNodes: [ \"localhost:3344\" ]" + NL
                + "      type: STATIC" + NL
                + "      nameResolutionAttempts: 10" + NL
                + "    }" + NL
                + "  }" + NL
                + "}";

        IgniteServer igniteServer = TestIgnitionManager.startWithProductionDefaults(testNodeName(testInfo, 0), config, workDir);

        try {
            igniteServer.initCluster(InitParameters.builder().clusterName("name").build());
            Ignite ignite = igniteServer.api();

            Path configFile = workDir.resolve(TestIgnitionManager.DEFAULT_CONFIG_NAME);
            String storedConfig = Files.readString(configFile);
            Config hoconConfig = ConfigFactory.parseString(config);
            assertThat(ConfigFactory.parseString(storedConfig), is(hoconConfig));

            IgniteImpl igniteImpl = TestWrappers.unwrapIgniteImpl(ignite);
            ConfigurationRegistry nodeCfgRegistry = igniteImpl.nodeConfiguration();
            HoconPresentation hoconPresentation = new HoconPresentation(nodeCfgRegistry);

            String configUpdate = "ignite {" + NL
                    + "  rest: {" + NL
                    + "    httpToHttpsRedirection: true" + NL
                    + "  }" + NL
                    + "}" + NL;

            CompletableFuture<Void> update = hoconPresentation.update(configUpdate);

            assertThat(update, willCompleteSuccessfully());
            storedConfig = Files.readString(configFile);

            assertThat(ConfigFactory.parseString(storedConfig), is(hoconConfig.withFallback(ConfigFactory.parseString(configUpdate))));
        } finally {
            igniteServer.shutdown();
        }
    }
}
