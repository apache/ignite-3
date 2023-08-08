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

package org.apache.ignite.internal.util;

import static java.util.stream.Collectors.toList;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Comparator;
import java.util.List;

/**
 * Files utilities.
 */
public class FilesUtils {

    /**
     * Sorts files by names.
     *
     * @param files Files.
     * @return Sorted files.
     */
    public static List<File> sortByNames(File... files) {
        return sortByNames(List.of(files));
    }

    /**
     * Sorts files by names.
     *
     * @param files Files.
     * @return Sorted files.
     */
    public static List<File> sortByNames(List<File> files) {
        return files.stream()
                .sorted(Comparator.comparing(File::getName))
                .collect(toList());
    }

    /**
     * Deletes directory if it exists.
     *
     * @param directory Directory to delete.
     * @return {@code true} if directory existed and was deleted.
     * @throws IOException If failed.
     */
    public static boolean deleteDirectoryIfExists(Path directory) throws IOException {
        if (!Files.exists(directory)) {
            return false;
        }

        Files.walkFileTree(directory, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });

        return true;
    }
}
