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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;

import java.nio.file.Path;
import org.apache.ignite.configuration.validation.ConfigurationValidationException;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Integration test for checking SSL configuration validation.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class ItSslConfigurationValidationTest {
    @ParameterizedTest
    @ValueSource(strings = {"clientConnector", "network", "rest"})
    void clientConnector(String rootKey, TestInfo testInfo, @WorkDirectory Path workDir) {
        String config = "{\n"
                + "  " + rootKey + ": {\n"
                + "    ssl: {\n"
                + "      enabled: true,\n"
                + "      clientAuth: none,\n"
                + "      keyStore: {\n"
                + "        path: \"bad_path\"\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}";

        assertThrowsWithCause(
                () -> TestIgnitionManager.start(testNodeName(testInfo, 0), config, workDir),
                ConfigurationValidationException.class,
                "Validation did not pass for keys: [" + rootKey + ".ssl.keyStore, Key store file doesn't exist at bad_path]");
    }
}
