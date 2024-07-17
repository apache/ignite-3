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
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.jar.JarFile;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.client.ClientArchTest.ModuleAndDependenciesClassPathProvider;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Assertions;

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

    private static JavaClasses clientModuleClasses() {
        return new ClassFileImporter().importPath(CLASS_PATH_DIR);
    }

    @Nullable
    private static JarFile toJarFile(Path path) {
        try {
            return new JarFile(path.toFile());
        } catch (IOException e) {
            return null;
        }
    }

    static class ModuleAndDependenciesClassPathProvider implements LocationProvider {

        @Override
        public Set<Location> get(Class<?> clazz) {
            // Running this test in IDE is not supported.
            // The cpFile is not set up properly, and the test skips.
            assumeTrue(CLASS_PATH_DIR != null);

            Path cp = Path.of(CLASS_PATH_DIR);

            Set<Location> result = new HashSet<>();
            result.add(Location.of(cp));

            try (Stream<Path> walk = Files.walk(cp)) {
                walk.map(ClientArchTest::toJarFile)
                        .filter(Objects::nonNull)
                        .map(Location::of)
                        .collect(Collectors.toCollection(() -> result));
            } catch (IOException e) {
                Assertions.fail("Failed " + e);
            }
            return result;
        }
    }
}
