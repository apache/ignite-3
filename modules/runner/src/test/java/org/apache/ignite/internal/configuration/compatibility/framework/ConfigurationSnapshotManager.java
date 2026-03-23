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

package org.apache.ignite.internal.configuration.compatibility.framework;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;
import org.apache.ignite.internal.util.io.IgniteUnsafeDataInput;
import org.apache.ignite.internal.util.io.IgniteUnsafeDataOutput;

/**
 * Manager for configuration snapshots used in compatibility tests.
 */
public class ConfigurationSnapshotManager {
    /**
     * Loads configuration trees from the given snapshot resource path.
     */
    public static List<ConfigNode> loadSnapshotFromResource(String path) throws IOException {
        try (InputStream is = ConfigurationSnapshotManager.class.getClassLoader().getResourceAsStream(path)) {
            if (is == null) {
                throw new IllegalArgumentException("Resource does not exist: " + path);
            }

            try (ZipInputStream zinStream = new ZipInputStream(is)) {
                zinStream.getNextEntry(); // Read the first entry in the zip file.

                IgniteUnsafeDataInput input = new IgniteUnsafeDataInput();
                input.inputStream(zinStream);

                List<ConfigNode> restoredNodes = ConfigNodeSerializer.readAsJson(input);

                assert zinStream.getNextEntry() == null : "More than one entry in the snapshot file";

                return restoredNodes;
            }
        }
    }

    /**
     * Saves the given configuration trees to a snapshot file.
     */
    public static void saveSnapshotToFile(List<ConfigNode> trees, Path file) throws IOException {
        Files.createDirectories(file.getParent());

        try (OutputStream foStream =
                Files.newOutputStream(file, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            ZipOutputStream zoutStream = new ZipOutputStream(foStream);
            zoutStream.putNextEntry(new ZipEntry("config_snapshot.json"));

            IgniteUnsafeDataOutput output = new IgniteUnsafeDataOutput(1024);
            output.outputStream(zoutStream);

            ConfigNodeSerializer.writeAsJson(trees, output);

            zoutStream.finish();
            output.flush();
        }
    }
}
