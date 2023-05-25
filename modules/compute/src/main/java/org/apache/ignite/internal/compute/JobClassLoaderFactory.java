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
import java.util.List;
import java.util.ListIterator;
import java.util.stream.Stream;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.version.Version;
import org.apache.ignite.lang.ErrorGroups.Compute;
import org.apache.ignite.lang.IgniteException;

/**
 * Creates a class loader for a job.
 */
public class JobClassLoaderFactory {

    /**
     * Directory for units.
     */
    private final Path unitsDir;

    /**
     * Constructor.
     *
     * @param unitsDir The directory for units.
     */
    public JobClassLoaderFactory(Path unitsDir) {
        this.unitsDir = unitsDir;
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

        ClassLoader parent = getClass().getClassLoader();
        ListIterator<DeploymentUnit> listIterator = units.listIterator(units.size());
        while (listIterator.hasPrevious()) {
            parent = createClassLoader(listIterator.previous(), parent);
        }

        return (JobClassLoader) parent;
    }

    private JobClassLoader createClassLoader(DeploymentUnit unit, ClassLoader parent) {
        return new JobClassLoader(collectClasspath(constructPath(unit)), parent);
    }

    private Path constructPath(DeploymentUnit unit) {
        if (unit.version().equals(Version.LATEST)) {
            try (Stream<Path> stream = Files.list(unitsDir.resolve(unit.name()))) {
                Version maxVersion = stream
                        .map(Path::getFileName)
                        .map(Path::toString)
                        .map(Version::parse)
                        .max(Version::compareTo)
                        .orElseThrow((() -> new IgniteException(Compute.CLASS_PATH_ERR, "Latest version not found: " + unit)));
                return unitsDir.resolve(unit.name()).resolve(maxVersion.toString());
            } catch (IOException e) {
                throw new IgniteException(
                        Compute.CLASS_PATH_ERR,
                        "Failed to construct path of the unit: " + unit,
                        e
                );
            }
        } else {
            return unitsDir.resolve(unit.name()).resolve(unit.version().toString());
        }
    }

    private static URL[] collectClasspath(Path unitDir) {
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
            return classpathCollector.classpath();
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

        URL[] classpath() {
            return classpath.toArray(new URL[0]);
        }
    }
}
