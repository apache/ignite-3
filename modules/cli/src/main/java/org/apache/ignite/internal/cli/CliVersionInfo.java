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

package org.apache.ignite.internal.cli;

import jakarta.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.ignite.internal.cli.core.exception.IgniteCliException;

/**
 * Provider of current Ignite CLI version info from the builtin properties file.
 */
@Singleton
public class CliVersionInfo {
    /** Ignite CLI version. */
    private final String version;

    /** Ignite CLI product. */
    private final String product;

    /**
     * Creates Ignite CLI version provider according to builtin version file.
     */
    public CliVersionInfo() {
        try (InputStream inputStream = CliVersionInfo.class.getResourceAsStream("/ignite.cli.version.properties")) {
            Properties prop = new Properties();
            prop.load(inputStream);

            version = prop.getProperty("version", "undefined");
            product = prop.getProperty("product", "undefined");
        } catch (IOException e) {
            throw new IgniteCliException("Can't read version info");
        }
    }

    String version() {
        return version;
    }

    String product() {
        return product;
    }
}
