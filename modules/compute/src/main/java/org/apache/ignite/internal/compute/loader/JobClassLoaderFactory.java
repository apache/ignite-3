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

package org.apache.ignite.internal.compute.loader;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.apache.ignite.compute.ComputeException;
import org.apache.ignite.internal.deployunit.DisposableDeploymentUnit;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.lang.ErrorGroups.Compute;

/**
 * Creates a class loader for a job.
 */
public class JobClassLoaderFactory {
    private static final IgniteLogger LOG = Loggers.forClass(JobClassLoaderFactory.class);

    /**
     * Create a class loader for the specified units.
     *
     * @param units The units of the job.
     * @return The class loader.
     */
    public JobClassLoader createClassLoader(List<DisposableDeploymentUnit> units) {
        return AccessController.doPrivileged(new PrivilegedAction<>() {
            @Override
            public JobClassLoader run() {
                URL[] classpath = units.stream()
                        .map(DisposableDeploymentUnit::path)
                        .flatMap(JobClassLoaderFactory::collectClasspath)
                        .toArray(URL[]::new);

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Created class loader with classpath: {}", Arrays.toString(classpath));
                }

                return new JobClassLoader(units, classpath, getClass().getClassLoader());
            }
        });
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
            throw new ComputeException(
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
