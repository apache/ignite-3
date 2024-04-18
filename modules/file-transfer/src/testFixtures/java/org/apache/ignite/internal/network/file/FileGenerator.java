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

package org.apache.ignite.internal.network.file;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

/**
 * File generator.
 */
public class FileGenerator {
    /**
     * Generates a random file with the given content size.
     *
     * @param dir Directory to create the file in.
     * @param contentSize Size of the file content.
     * @return Path to the generated file.
     */
    public static Path randomFile(Path dir, int contentSize) {
        try {
            Path tempFile = Files.createTempFile(dir, null, null);
            byte[] bytes = new byte[contentSize];
            new Random().nextBytes(bytes);
            return Files.write(tempFile, bytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
