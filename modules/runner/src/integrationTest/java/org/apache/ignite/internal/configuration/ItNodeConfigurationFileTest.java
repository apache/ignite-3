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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import java.util.UUID;
import org.apache.ignite.configuration.validation.ConfigurationValidationException;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.lang.IgniteException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * General integration tests for node configuration file.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class ItNodeConfigurationFileTest {

    @Test
    public void wrongConfigurationFormat(TestInfo testInfo, @WorkDirectory Path workDir) {
        String config = UUID.randomUUID().toString();

        IgniteException igniteException = assertThrows(
                IgniteException.class,
                () -> TestIgnitionManager.start(testNodeName(testInfo, 0), config, workDir)
        );
        assertThat(igniteException.getCause(), is(instanceOf(NodeConfigParseException.class)));
        assertThat(igniteException.getCause().getMessage(), containsString("Failed to parse config content from file "));
    }

    @Test
    public void illegalConfigurationValueType(TestInfo testInfo, @WorkDirectory Path workDir) {
        String config =
                "{\n"
                + "  rest: {\n"
                + "    ssl: {\n"
                + "      enabled: true,\n"
                + "      clientAuth: none,\n"
                + "      keyStore: {\n"
                + "        path: 123\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}";

        assertThrowsWithCause(
                () -> TestIgnitionManager.start(testNodeName(testInfo, 0), config, workDir),
                ConfigurationValidationException.class,
                "'String' is expected as a type for the 'rest.ssl.keyStore.path' configuration value");
    }

    @Test
    public void illegalConfigurationValue(TestInfo testInfo, @WorkDirectory Path workDir) {
        String config = "{\n"
                + "  rest: {\n"
                + "    ssl: {\n"
                + "      enabled: true,\n"
                + "      clientAuth: none,\n"
                + "      keyStore: {\n"
                + "        path: bad_path\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}";

        assertThrowsWithCause(
                () -> TestIgnitionManager.start(testNodeName(testInfo, 0), config, workDir),
                ConfigurationValidationException.class,
                "Validation did not pass for keys: [rest.ssl.keyStore, Key store file doesn't exist at bad_path]");
    }
}
