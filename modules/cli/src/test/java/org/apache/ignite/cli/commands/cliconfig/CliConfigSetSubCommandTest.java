/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.cli.commands.cliconfig;

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.inject.Inject;
import org.apache.ignite.cli.commands.CliCommandTestBase;
import org.apache.ignite.cli.config.Config;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class CliConfigSetSubCommandTest extends CliCommandTestBase {

    @Inject
    Config config;

    @BeforeEach
    void setUp() {
        setUp(CliConfigSetSubCommand.class);
    }

    @Test
    @DisplayName("Need at least one parameter")
    void noKey() {
        // When executed without arguments
        execute();

        // Then
        assertThat(err.toString()).contains("Missing required parameter");
        // And
        assertThat(out.toString()).isEmpty();
    }

    @Test
    @DisplayName("Missing value")
    void singleKey() {
        // When executed with key
        execute("ignite.cluster-url");

        // Then
        assertThat(err.toString()).contains("should be in KEY=VALUE format but was ignite.cluster-url");
        // And
        assertThat(out.toString()).isEmpty();
    }

    @Test
    @DisplayName("Sets single value")
    void singleKeySet() {
        // When executed with key
        execute("ignite.cluster-url=test");

        // Then
        assertThat(out.toString()).isEmpty();
        // And
        assertThat(err.toString()).isEmpty();
        // And
        assertThat(config.getProperty("ignite.cluster-url")).isEqualTo("test");
    }

    @Test
    @DisplayName("Sets multiple values")
    void multipleKeys() {
        // When executed with multiple keys
        execute("ignite.cluster-url=test", "ignite.jdbc-url=test2");

        // Then
        assertThat(out.toString()).isEmpty();
        // And
        assertThat(err.toString()).isEmpty();
        // And
        assertThat(config.getProperty("ignite.cluster-url")).isEqualTo("test");
        // And
        assertThat(config.getProperty("ignite.jdbc-url")).isEqualTo("test2");
    }
}
