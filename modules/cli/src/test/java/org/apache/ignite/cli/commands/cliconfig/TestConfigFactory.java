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

import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Replaces;
import jakarta.inject.Singleton;
import java.io.File;
import java.io.IOException;
import org.apache.ignite.cli.config.Config;
import org.apache.ignite.cli.config.ConfigFactory;

/**
 * Test factory for {@link Config}.
 */
@Factory
@Replaces(factory = ConfigFactory.class)
public class TestConfigFactory {

    /**
     * Creates a {@link Config} with some defaults for testing.
     *
     * @return {@link Config}
     * @throws IOException in case temp file couldn't be created
     */
    @Singleton
    public Config createConfig() throws IOException {
        File tempFile = File.createTempFile("cli", null);
        tempFile.deleteOnExit();
        Config config = new Config(tempFile);
        config.setProperty("ignite.cluster-url", "test_cluster_url");
        config.setProperty("ignite.jdbc-url", "test_jdbc_url");
        return config;
    }
}
