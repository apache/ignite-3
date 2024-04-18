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

package org.apache.ignite.internal.cli.config.ini;

import static java.nio.file.Files.lines;
import static java.nio.file.attribute.PosixFilePermission.GROUP_READ;
import static java.nio.file.attribute.PosixFilePermission.OWNER_EXECUTE;
import static java.nio.file.attribute.PosixFilePermission.OWNER_READ;
import static java.nio.file.attribute.PosixFilePermission.OWNER_WRITE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.cli.config.CliConfigKeys;
import org.apache.ignite.internal.cli.core.exception.IgniteCliException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

class IniConfigManagerTest {

    File configFile;
    File secretConfigFile;

    @BeforeEach
    void setUp(@TempDir File configDir) {
        configFile = new File(configDir, "config.ini");
        secretConfigFile = new File(configDir, "secret.ini");
    }

    @Test
    void managerWritesConfigKeysProperly() throws IOException {
        // when
        IniConfigManager manager = new IniConfigManager(configFile, secretConfigFile);

        Arrays.stream(CliConfigKeys.values())
                .map(CliConfigKeys::value)
                .forEach(it -> {
                    manager.getCurrentProfile().setProperty(it, "1234");
                });

        String[] secretKeys = CliConfigKeys.secretConfigKeys().toArray(new String[0]);

        // then secret config file contains secret keys
        List<String> secretConfigFileKeys = readKeysFromFile(secretConfigFile, 2);

        assertThat(secretConfigFileKeys, hasSize(secretKeys.length));
        assertThat(secretConfigFileKeys, containsInAnyOrder(secretKeys));

        // and config file does not contain secret keys
        List<String> configFileKeys = readKeysFromFile(configFile, 3);
        String[] notSecretConfigKeys = Arrays.stream(CliConfigKeys.values())
                .map(CliConfigKeys::value)
                .filter(it -> !CliConfigKeys.secretConfigKeys().contains(it))
                .toArray(String[]::new);

        assertThat(configFileKeys, hasSize(notSecretConfigKeys.length));
        assertThat(configFileKeys, containsInAnyOrder(notSecretConfigKeys));

    }

    @Test
    @DisabledOnOs(OS.WINDOWS)
    void secretConfigFileIsCreatedWithCorrectPermissions() throws IOException {
        // when
        new IniConfigManager(configFile, secretConfigFile);

        // then
        Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(secretConfigFile.toPath());
        assertThat(permissions, containsInAnyOrder(OWNER_READ, OWNER_WRITE, OWNER_EXECUTE));
    }

    @Test
    @DisabledOnOs(OS.WINDOWS)
    void managerValidatesSecretConfigFilePermissions() throws IOException {
        // when
        Files.createFile(secretConfigFile.toPath(), PosixFilePermissions.asFileAttribute(Set.of(OWNER_READ, GROUP_READ)));

        // then
        IgniteCliException igniteCliException = assertThrows(IgniteCliException.class,
                () -> new IniConfigManager(configFile, secretConfigFile));
        assertThat(igniteCliException.getMessage(),
                containsString("The secret configuration file must have 700 permissions"));
    }

    private static List<String> readKeysFromFile(File file, int headerLength) throws IOException {
        try (Stream<String> lines = lines(file.toPath())) {
            return lines.skip(headerLength) // skip header
                    .map(it -> it.split("="))
                    .map(it -> it[0].trim())
                    .filter(it -> !it.isBlank())
                    .collect(Collectors.toList());
        }
    }
}
