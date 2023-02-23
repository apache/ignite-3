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

package org.apache.ignite.client;

import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.domain.JavaModifier;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.Location;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.junit.LocationProvider;
import com.tngtech.archunit.lang.syntax.ArchRuleDefinition;
import com.tngtech.archunit.lang.syntax.elements.FieldsShouldConjunction;
import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.jar.JarFile;
import java.util.stream.Collectors;
import org.apache.ignite.client.ClientArchTest.ModuleAndDependenciesClassPathProvider;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.jetbrains.annotations.NotNull;

/**
 * This test ensures static logger is not used in the modules the client module relies on.
 *
 * <p>It's possible to specify a custom logger for an Ignite client, and that configured logger should be used everywhere throughout
 * a client, that's why we shouldn't use a static logger.
 */
@AnalyzeClasses(locations = ModuleAndDependenciesClassPathProvider.class)
public class ClientArchTest {

    private static final String CLASS_PATH_DIR = System.getProperty("archtest.dir");

    @ArchTest
    FieldsShouldConjunction noStaticIgniteLogger = ArchRuleDefinition.noFields()
            .that().haveRawType(IgniteLogger.class)
            .should().haveModifier(JavaModifier.STATIC);

    @NotNull
    private static JarFile jarFile(File f) {
        try {
            return new JarFile(f);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static JavaClasses clientModuleClasses() {
        return new ClassFileImporter().importPath(CLASS_PATH_DIR);
    }

    static class ModuleAndDependenciesClassPathProvider implements LocationProvider {

        @NotNull
        private static Set<Location> merge(Set<Location> moduleDependencies, Location moduleClasses) {
            Set<Location> classPath = new HashSet<>(moduleDependencies.size() + 1);
            classPath.add(moduleClasses);
            classPath.addAll(moduleDependencies);
            return classPath;
        }

        @Override
        public Set<Location> get(Class<?> clazz) {
            // Running this test in IDE is not supported.
            // The cpFile is not set up properly, and the test skips.
            assumeTrue(CLASS_PATH_DIR != null);

            Path cp = Path.of(CLASS_PATH_DIR);
            File cpFile = cp.toFile();
            File[] cpFiles = cpFile.listFiles();
            if (cpFiles == null) {
                fail("Failed to list files in " + cpFile.getAbsolutePath());
            }

            Set<Location> moduleDependencies = Arrays.stream(cpFiles)
                    .filter(f -> f.getName().endsWith(".jar"))
                    .map(ClientArchTest::jarFile)
                    .map(Location::of)
                    .collect(Collectors.toSet());

            Location moduleClasses = Location.of(cp);

            return merge(moduleDependencies, moduleClasses);
        }
    }
}
