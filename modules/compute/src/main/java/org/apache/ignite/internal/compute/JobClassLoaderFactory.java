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

package org.apache.ignite.internal.compute;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.version.Version;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.lang.ErrorGroups.Compute;
import org.apache.ignite.lang.IgniteException;

/**
 * Creates a class loader for a job.
 */
public class JobClassLoaderFactory {
    private static final IgniteLogger LOG = Loggers.forClass(JobClassLoaderFactory.class);

    /**
     * Directory for units.
     */
    private final Path unitsDir;

    /**
     * The function to detect the last version of the unit.
     */
    private final Function<String, Version> detectLastUnitVersion;

    /**
     * Constructor.
     *
     * @param unitsDir The directory for units.
     * @param detectLastUnitVersion The function to detect the last version of the unit.
     */
    public JobClassLoaderFactory(Path unitsDir, Function<String, Version> detectLastUnitVersion) {
        this.unitsDir = unitsDir;
        this.detectLastUnitVersion = detectLastUnitVersion;
    }

    /**
     * Create a class loader for the specified units.
     *
     * @param units The units of the job.
     * @return The class loader.
     */
    public JobClassLoader createClassLoader(List<DeploymentUnit> units) {
        if (units.isEmpty()) {
            throw new IllegalArgumentException("At least one unit must be specified");
        }

        URL[] classPath = units.stream()
                .map(this::constructPath)
                .flatMap(JobClassLoaderFactory::collectClasspath)
                .toArray(URL[]::new);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Classpath for job: " + Arrays.toString(classPath));
        }

        return new JobClassLoader(classPath, getClass().getClassLoader());
    }

    private Path constructPath(DeploymentUnit unit) {
        Version version = unit.version() == Version.LATEST ? detectLastUnitVersion.apply(unit.name()) : unit.version();
        return unitsDir.resolve(unit.name()).resolve(version.toString());
    }

    private static Stream<URL> collectClasspath(Path unitDir) {
        if (Files.notExists(unitDir)) {
            throw new IllegalArgumentException("Unit does not exist: " + unitDir);
        }

        if (!Files.isDirectory(unitDir)) {
            throw new IllegalArgumentException("Unit is not a directory: " + unitDir);
        }

        // Construct the "class path" for this unit
        try {
            ClasspathCollector classpathCollector = new ClasspathCollector(unitDir);
            Files.walkFileTree(unitDir, classpathCollector);
            return classpathCollector.classpathAsStream();
        } catch (IOException e) {
            throw new IgniteException(
                    Compute.CLASS_PATH_ERR,
                    "Failed to construct classpath for job: " + unitDir,
                    e
            );
        }
    }

    private static class ClasspathCollector extends SimpleFileVisitor<Path> {
        private final List<URL> classpath = new ArrayList<>();

        private ClasspathCollector(Path base) throws MalformedURLException {
            classpath.add(base.toAbsolutePath().toUri().toURL());
        }

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
            if (Files.isDirectory(file) || file.toString().endsWith(".jar")) {
                classpath.add(file.toAbsolutePath().toUri().toURL());
            }
            return FileVisitResult.CONTINUE;
        }

        Stream<URL> classpathAsStream() {
            return classpath.stream();
        }
    }
}
