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

package org.apache.ignite.internal;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.gradle.tooling.GradleConnectionException;
import org.gradle.tooling.GradleConnector;
import org.gradle.tooling.ProjectConnection;

/**
 * Utility class for resolving dependencies using Gradle.
 */
public class Dependencies {
    private static final IgniteLogger LOG = Loggers.forClass(Dependencies.class);

    private Dependencies() {}

    /**
     * Provides the path to the dependency(s). Uses ; to separate jar files.
     */
    public static String path(String dependencyNotation, boolean transitive, boolean currentVersion) {
        LOG.info("Resolving path for dependency: " + dependencyNotation);
        File projectRoot = getProjectRoot();

        if (currentVersion) {
            String local = path(projectRoot, dependencyNotation);
            if (!local.isEmpty()) {
                return local;
            }
        }

        try (ProjectConnection connection = GradleConnector.newConnector()
                .forProjectDirectory(projectRoot)
                .connect()
        ) {
            File file = constructArgFile(connection, dependencyNotation, true, transitive);
            return Files.readAllLines(file.toPath()).stream()
                    .map(String::trim)
                    .collect(Collectors.joining(";"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static String path(File root, String dependencyNotation) {
        String[] s = dependencyNotation.split(":");
        if (s.length < 3) {
            throw new RuntimeException("Supplied String module notation '" + dependencyNotation
                    + "' is invalid. Example notations: 'org.gradle:gradle-core:2.2'");
        }

        String name = s[1];
        String version = s[2];
        String nameRegex = name + "-" + version + "\\.jar";

        try (Stream<Path> stream = Files.walk(root.toPath())) {
            return stream
                    .filter(Files::isRegularFile)
                    .map(Path::toAbsolutePath)
                    .filter(path -> path.getFileName().toString().matches(nameRegex))
                    .map(Path::toString)
                    .findFirst()
                    .orElseGet(() -> {
                        LOG.info("Unable to find dependency in build dir " + dependencyNotation);
                        return "";
                    });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static File constructArgFile(
            ProjectConnection connection,
            String dependencyNotation,
            boolean classPathOnly
    ) throws IOException {
        return constructArgFile(connection, dependencyNotation, classPathOnly, true);
    }

    static File constructArgFile(
            ProjectConnection connection,
            String dependencyNotation,
            boolean classPathOnly,
            boolean transitive
    ) throws IOException {
        File argFilePath = File.createTempFile("argFilePath", "");
        argFilePath.deleteOnExit();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try {
            connection.newBuild()
                    .forTasks(":ignite-compatibility-tests:constructArgFile")
                    .withArguments(
                            "-PdependencyNotation=" + dependencyNotation,
                            "-PargFilePath=" + argFilePath,
                            "-PargFileTransitive=" + transitive,
                            "-PargFileClassPathOnly=" + classPathOnly
                    )
                    .setStandardOutput(baos)
                    .setStandardError(baos)
                    .run();
        } catch (GradleConnectionException | IllegalStateException e) {
            LOG.error("Failed to run constructArgFile task", e);
            LOG.error("Gradle task output:" + System.lineSeparator() + baos);
            throw new RuntimeException(e);
        }

        return argFilePath;
    }

    static File getProjectRoot() {
        var absPath = new File("").getAbsolutePath();

        // Find root by looking for "gradlew" file.
        while (!new File(absPath, "gradlew").exists()) {
            var parent = new File(absPath).getParentFile();
            if (parent == null) {
                throw new IllegalStateException("Could not find project root with 'gradlew' file");
            }
            absPath = parent.getAbsolutePath();
        }

        return new File(absPath);
    }
}
