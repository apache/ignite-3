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

package org.apache.ignite.internal.fileio;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.OpenOption;
import java.nio.file.Path;

/**
 * {@link FileIo} factory definition.
 */
@FunctionalInterface
public interface FileIoFactory extends Serializable {
    /**
     * Creates I/O interface for file with default I/O mode.
     *
     * @param filePath File path.
     * @throws IOException If I/O interface creation was failed.
     */
    default FileIo create(Path filePath) throws IOException {
        return create(filePath, CREATE, READ, WRITE);
    }

    /**
     * Creates I/O interface for file with specified mode.
     *
     * @param filePath File path.
     * @param modes Open modes.
     * @throws IOException If I/O interface creation was failed.
     */
    FileIo create(Path filePath, OpenOption... modes) throws IOException;
}
