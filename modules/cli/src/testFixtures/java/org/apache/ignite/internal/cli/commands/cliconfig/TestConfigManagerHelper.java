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

package org.apache.ignite.internal.cli.commands.cliconfig;

import static java.nio.file.attribute.PosixFilePermission.OWNER_EXECUTE;
import static java.nio.file.attribute.PosixFilePermission.OWNER_READ;
import static java.nio.file.attribute.PosixFilePermission.OWNER_WRITE;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;
import org.apache.ignite.internal.cli.config.ConfigManager;
import org.apache.ignite.internal.cli.util.OperatingSystem;

/**
 * Test factory for {@link ConfigManager}.
 */
public class TestConfigManagerHelper {
    private static final String EMPTY = "empty.ini";
    private static final String EMPTY_SECRET = "empty_secret.ini";
    private static final String ONE_SECTION_WITH_DEFAULT_PROFILE = "one_section_with_default_profile.ini";
    private static final String TWO_SECTION_WITH_DEFAULT_PROFILE = "two_section_with_default_profile.ini";
    private static final String TWO_SECTION_WITHOUT_DEFAULT_PROFILE = "two_section_without_default_profile.ini";
    private static final String INTEGRATION_TESTS = "integration_tests.ini";

    private static final String CLUSTER_URL_NON_DEFAULT = "cluster_url_non_default.ini";

    private static final String CLUSTER_URL_SSL = "cluster_url_ssl.ini";

    public static File createEmptyConfig() {
        return copyResourceToTempFile(EMPTY);
    }

    /** Creates and returns the empty secret file config. */
    public static File createEmptySecretConfig() {
        File file = copyResourceToTempFile(EMPTY_SECRET);
        try {
            setFilePermissions(file, Set.of(OWNER_READ, OWNER_WRITE, OWNER_EXECUTE));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return file;
    }

    public static File createOneSectionWithDefaultProfile() {
        return copyResourceToTempFile(ONE_SECTION_WITH_DEFAULT_PROFILE);
    }

    public static File createSectionWithDefaultProfile() {
        return copyResourceToTempFile(TWO_SECTION_WITH_DEFAULT_PROFILE);
    }

    public static File createSectionWithoutDefaultProfile() {
        return copyResourceToTempFile(TWO_SECTION_WITHOUT_DEFAULT_PROFILE);
    }

    public static File createIntegrationTests() {
        return copyResourceToTempFile(INTEGRATION_TESTS);
    }

    public static File createClusterUrlNonDefault() {
        return copyResourceToTempFile(CLUSTER_URL_NON_DEFAULT);
    }

    public static File createClusterUrlSsl() {
        return copyResourceToTempFile(CLUSTER_URL_SSL);
    }

    /**
     * Helper method to copy file from the classpath to the temporary file which will be deleted on exit.
     *
     * @param resource The resource name
     * @return A temporary file containing the resource's contents
     */
    public static File copyResourceToTempFile(String resource) {
        try {
            File tempFile = File.createTempFile("cli", null);

            try (FileOutputStream fileOutputStream = new FileOutputStream(tempFile)) {
                FileChannel dest = fileOutputStream.getChannel();
                InputStream resourceAsStream = TestConfigManagerHelper.class.getClassLoader().getResourceAsStream(resource);
                ReadableByteChannel src = Channels.newChannel(resourceAsStream);
                dest.transferFrom(src, 0, Integer.MAX_VALUE);
                tempFile.deleteOnExit();
                return tempFile;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void setFilePermissions(File file, Set<PosixFilePermission> perms) throws IOException {
        if (OperatingSystem.current() != OperatingSystem.WINDOWS) {
            Files.setPosixFilePermissions(file.toPath(), perms);
        }
    }
}
