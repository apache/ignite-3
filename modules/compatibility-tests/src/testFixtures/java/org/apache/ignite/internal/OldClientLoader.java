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

import com.linkedin.cytodynamics.nucleus.DelegateRelationshipBuilder;
import com.linkedin.cytodynamics.nucleus.IsolationLevel;
import com.linkedin.cytodynamics.nucleus.LoaderBuilder;
import com.linkedin.cytodynamics.nucleus.OriginRestriction;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import org.gradle.tooling.GradleConnector;
import org.gradle.tooling.ProjectConnection;

/**
 * Class loader for old Ignite client versions.
 */
public class OldClientLoader {
    /**
     * Creates a classloader for the specified Ignite client version.
     *
     * @param igniteVersion The version of the Ignite client to load.
     * @return An isolated classloader containing the specified Ignite client version and its dependencies.
     *
     * @throws IOException In case of an I/O error while constructing the classpath.
     */
    public static ClassLoader getIsolatedClassLoader(String igniteVersion) throws IOException {
        // 1. Use constructArgFile to resolve dependencies of a given client version.
        // 2. Use Cytodynamics to run the client with the constructed arg file in an isolated classloader.
        try (ProjectConnection connection = GradleConnector.newConnector()
                .forProjectDirectory(Path.of("..", "..").normalize().toFile())
                .connect()
        ) {
            File argFile = Dependencies.constructArgFile(connection, "org.apache.ignite:ignite-client:" + igniteVersion, true);

            List<URI> classpath = Files.readAllLines(argFile.toPath())
                    .stream()
                    .map(path -> new File(path).toURI())
                    .collect(Collectors.toList());

            // Add tests to the classpath.
            classpath.add(Path.of("build", "classes", "java", "testFixtures").toUri());
            classpath.add(Path.of("build", "classes", "java", "integrationTest").toUri());

            return LoaderBuilder
                    .anIsolatingLoader()
                    .withClasspath(classpath)
                    .withOriginRestriction(OriginRestriction.allowByDefault())
                    .withParentRelationship(DelegateRelationshipBuilder.builder()
                            .withIsolationLevel(IsolationLevel.FULL)
                            .addWhitelistedClassPredicate(x -> !x.startsWith("org.apache.ignite."))
                            .build())
                    .build();
        }
    }
}
