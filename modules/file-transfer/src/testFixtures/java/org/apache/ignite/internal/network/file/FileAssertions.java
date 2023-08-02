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

import static org.apache.ignite.internal.testframework.matchers.FileContentMatcher.hasContent;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * File assertions.
 */
public class FileAssertions {

    /**
     * Asserts that the content of the two directories is the same.
     *
     * @param expected Expected directory.
     * @param actual Actual directory.
     */
    public static void assertContentEquals(Path expected, Path actual) {
        try {
            Files.walkFileTree(expected, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                    Path otherDir = actual.resolve(expected.relativize(dir));
                    if (!Files.exists(otherDir)) {
                        throw new AssertionError("Directory " + dir + " does not exist in " + actual);
                    }

                    try (Stream<Path> expectedFilesStream = Files.list(dir);
                            Stream<Path> actualFilesStream = Files.list(otherDir)) {
                        List<String> expectedFiles = expectedFilesStream
                                .map(Path::toFile)
                                .map(File::getName)
                                .collect(Collectors.toList());

                        List<String> actualFiles = actualFilesStream
                                .map(Path::toFile)
                                .map(File::getName)
                                .collect(Collectors.toList());

                        assertThat(expectedFiles, is(actualFiles));
                    }
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                    Path otherFile = actual.resolve(expected.relativize(file));
                    if (Files.exists(otherFile)) {
                        assertThat(file, hasContent(otherFile));
                    } else {
                        throw new AssertionError("File " + file + " does not exist in " + actual);
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            throw new AssertionError("Failed to compare directories", e);
        }
    }
}
