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
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.version.Version;
import org.apache.ignite.internal.deployunit.IgniteDeployment;
import org.apache.ignite.internal.deployunit.exception.DeploymentUnitNotFoundException;
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
     * The deployer service.
     */
    private final IgniteDeployment deployment;

    /**
     * Constructor.
     *
     * @param deployment The deployer service.
     */
    public JobClassLoaderFactory(IgniteDeployment deployment) {
        this.deployment = deployment;
    }

    /**
     * Create a class loader for the specified units.
     *
     * @param units The units of the job.
     * @return The class loader.
     */
    public CompletableFuture<JobClassLoader> createClassLoader(List<DeploymentUnit> units) {
        Stream<URL>[] unitUrls = new Stream[units.size()];

        CompletableFuture[] futures = IntStream.range(0, units.size())
                .mapToObj(id -> {
                    return constructPath(units.get(id))
                            .thenApply(JobClassLoaderFactory::collectClasspath)
                            .thenAccept(stream -> unitUrls[id] = stream);
                }).toArray(CompletableFuture[]::new);

        return CompletableFuture.allOf(futures).thenApply(v -> {
            return Stream.of(unitUrls)
                    .flatMap(Function.identity())
                    .toArray(URL[]::new);
        })
                .thenApply(it -> new JobClassLoader(it, getClass().getClassLoader()))
                .whenComplete((cl, err) -> {
                    if (err != null) {
                        LOG.error("Failed to create class loader", err);
                    } else {
                        LOG.debug("Created class loader: {}", cl);
                    }
                });
    }

    private CompletableFuture<Path> constructPath(DeploymentUnit unit) {
        return CompletableFuture.completedFuture(unit.version())
                .thenCompose(version -> {
                    if (version == Version.LATEST) {
                        return lastVersion(unit.name());
                    } else {
                        return CompletableFuture.completedFuture(version);
                    }
                })
                .thenCompose(version -> deployment.path(unit.name(), version));
    }

    private CompletableFuture<Version> lastVersion(String name) {
        return deployment.versionsAsync(name)
                .thenApply(versions -> {
                    return versions.stream()
                            .max(Version::compareTo)
                            .orElseThrow(() -> new DeploymentUnitNotFoundException("No versions found for deployment unit: " + name));
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
