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
import java.util.Arrays;
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
    private static final byte[] IGNITE_COMPRESSED_MAGIC = "IGNZ".getBytes();
    private static final int FORMAT_VERSION = 1;

    /**
     * Loads configuration trees from the given snapshot file.
     */
    public static List<ConfigNode> loadSnapshot(Path snapshotFile) throws IOException {
        assert Files.isRegularFile(snapshotFile);

        List<ConfigNode> restoredNodes;
        try (InputStream finStream = Files.newInputStream(snapshotFile, StandardOpenOption.READ)) {
            byte[] magic = finStream.readNBytes(6);

            if (Arrays.equals(IGNITE_COMPRESSED_MAGIC, magic)) {
                int formatVersion = finStream.read();

                if (formatVersion != FORMAT_VERSION) {
                    throw new IllegalStateException("Unsupported format version: " + formatVersion);
                }

                ZipInputStream zinStream = new ZipInputStream(finStream);
                zinStream.getNextEntry(); // Read the first entry in the zip file.

                IgniteUnsafeDataInput input = new IgniteUnsafeDataInput();
                input.inputStream(zinStream);

                restoredNodes = ConfigNodeSerializer.readAll(input);

                assert zinStream.getNextEntry() == null : "More than one entry in the snapshot file";
            } else {
                throw new IllegalStateException("Not a configuration snapshot file.");
            }
        }
        return restoredNodes;
    }

    /**
     * Saves the given configuration trees to a snapshot file.
     */
    public static void saveSnapshot(List<ConfigNode> trees, Path file) throws IOException {
        Files.createDirectories(file.getParent());

        try (OutputStream foStream =
                Files.newOutputStream(file, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            foStream.write(IGNITE_COMPRESSED_MAGIC);
            foStream.write(FORMAT_VERSION);

            ZipOutputStream zoutStream = new ZipOutputStream(foStream);
            zoutStream.putNextEntry(new ZipEntry(file.getFileName().toString()));

            IgniteUnsafeDataOutput output = new IgniteUnsafeDataOutput(1024);
            output.outputStream(zoutStream);

            ConfigNodeSerializer.writeAll(trees, output);

            zoutStream.finish();
            output.flush();
        }
    }
}
