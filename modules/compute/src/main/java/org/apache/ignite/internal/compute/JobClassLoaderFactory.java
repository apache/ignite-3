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
import org.apache.ignite.lang.ErrorGroups.Common;
import org.apache.ignite.lang.IgniteException;

/**
 * Creates a class loader for a job.
 */
public class JobClassLoaderFactory {

    /**
     * Directory for components.
     */
    private final Path componentsDir;

    /**
     * Constructor.
     *
     * @param componentsDir The directory for components.
     */
    public JobClassLoaderFactory(Path componentsDir) {
        this.componentsDir = componentsDir;
    }

    /**
     * Create a class loader for the specified units.
     *
     * @param units The units of the job.
     * @return The class loader.
     */
    public JobClassLoader createClassLoader(List<String> units) {
        if (units.isEmpty()) {
            throw new IllegalArgumentException("No units specified");
        }

        URL[] classPath = units.stream()
                .map(componentsDir::resolve)
                .map(JobClassLoaderFactory::constructClasspath)
                .flatMap(Arrays::stream)
                .toArray(URL[]::new);

        return new JobClassLoader(classPath, getClass().getClassLoader());
    }

    private static URL[] constructClasspath(Path componentDir) {
        if (Files.notExists(componentDir)) {
            throw new IllegalArgumentException("Component does not exist: " + componentDir);
        }

        if (!Files.isDirectory(componentDir)) {
            throw new IllegalArgumentException("Component is not a directory: " + componentDir);
        }

        // Construct the "class path" for this component
        try {
            ClasspathCollector classpathCollector = new ClasspathCollector(componentDir);
            Files.walkFileTree(componentDir, classpathCollector);
            return classpathCollector.classpath();
        } catch (IOException e) {
            throw new IgniteException(
                    Common.UNEXPECTED_ERR,
                    "Failed to construct classpath for component: " + componentDir,
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
