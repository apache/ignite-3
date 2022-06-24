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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import org.apache.ignite.cli.config.ConfigManager;

/**
 * Test factory for {@link ConfigManager}.
 */
public class TestConfigManagerHelper {
    public static final String EMPTY = "empty.ini";
    public static final String TWO_SECTION_WITH_INTERNAL_PART = "two_section_with_internal.ini";
    public static final String TWO_SECTION_WITHOUT_INTERNAL_PART = "two_section_without_internal.ini";
    public static final String INTEGRATION_TESTS = "integration_tests.ini";

    public static File createEmptyConfig() {
        return createIniFile(EMPTY);
    }

    public static File createSectionWithInternalPart() {
        return createIniFile(TWO_SECTION_WITH_INTERNAL_PART);
    }

    public static File createSectionWithoutInternalPart() {
        return createIniFile(TWO_SECTION_WITHOUT_INTERNAL_PART);
    }

    public static File createIntegrationTests() {
        return createIniFile(INTEGRATION_TESTS);
    }

    private static File createIniFile(String iniResource) {
        try {
            File tempFile = File.createTempFile("cli", null);

            try (FileOutputStream fileOutputStream = new FileOutputStream(tempFile)) {
                FileChannel dest = fileOutputStream.getChannel();
                InputStream resourceAsStream = TestConfigManagerHelper.class.getClassLoader().getResourceAsStream(iniResource);
                ReadableByteChannel src = Channels.newChannel(resourceAsStream);
                dest.transferFrom(src, 0, Integer.MAX_VALUE);
                tempFile.deleteOnExit();
                return tempFile;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
