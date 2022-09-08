/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.client;

import com.tngtech.archunit.core.domain.JavaModifier;
import com.tngtech.archunit.core.importer.Location;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.junit.LocationProvider;
import com.tngtech.archunit.lang.ArchRule;
import com.tngtech.archunit.lang.syntax.ArchRuleDefinition;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.jar.JarFile;
import org.apache.ignite.client.ClientArchTest.ClassesWithLibsLocationProvider;
import org.apache.ignite.internal.logger.IgniteLogger;

/**
 * This test ensures static logger is not used in the modules the client module relies on.
 *
 * <p>It's possible to specify a custom logger for an Ignite client, and that configured logger should be used everywhere throughout
 * a client, that's why we shouldn't use a static logger.
 */
@AnalyzeClasses(
        locations = ClassesWithLibsLocationProvider.class
)
public class ClientArchTest {
    static class ClassesWithLibsLocationProvider implements LocationProvider {
        @Override
        public Set<Location> get(Class<?> testClass) {
            var locations = new HashSet<Location>();

            // both target/classes and target/libs defines a runtime scope of this particular module
            locations.add(Location.of(directoryFromBuildDir("classes")));

            var libDir = directoryFromBuildDir("libs").toFile();

            for (var lib : libDir.listFiles()) {
                if (!lib.getName().endsWith(".jar")) {
                    continue;
                }

                try {
                    locations.add(Location.of(new JarFile(lib)));
                } catch (IOException e) {
                    throw new AssertionError("Unable to read jar file", e);
                }
            }

            return locations;
        }

        private static Path directoryFromBuildDir(String folder) {
            Path path = Paths.get("target", folder);
            if (!path.toFile().exists()) {
                path = Paths.get("build", folder);
            }
            if (!path.toFile().exists()) {
                throw new AssertionError("Expect " + folder + " directory to exist.");
            }
            return path;
        }
    }

    @ArchTest
    public static final ArchRule NO_STATIC_LOGGER_FIELD = ArchRuleDefinition.noFields()
            .that().haveRawType(IgniteLogger.class)
            .should().haveModifier(JavaModifier.STATIC);
}
