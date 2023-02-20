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

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.domain.JavaModifier;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.lang.syntax.ArchRuleDefinition;
import com.tngtech.archunit.lang.syntax.elements.FieldsShouldConjunction;
import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.jar.JarFile;
import java.util.stream.Collectors;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

/**
 * This test ensures static logger is not used in the modules the client module relies on.
 *
 * <p>It's possible to specify a custom logger for an Ignite client, and that configured logger should be used everywhere throughout
 * a client, that's why we shouldn't use a static logger.
 */
public class ClientArchTest {

    private static final Path CLASS_PATH_DIR = Path.of(System.getProperty("archtest.dir"));

    private static JavaClasses clientModuleDependencies() {
        File classpath = CLASS_PATH_DIR.toFile();
        File[] classpathFiles = classpath.listFiles();
        if (classpathFiles == null) {
            fail("Failed to list files in " + classpath.getAbsolutePath());
        }

        return new ClassFileImporter().importJars(
                Arrays.stream(classpathFiles)
                        .filter(f -> f.getName().endsWith(".jar"))
                        .map(ClientArchTest::jarFile)
                        .collect(Collectors.toList())
        );
    }

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

    @Test
    void noStaticIgniteLoggerDefined() {
        FieldsShouldConjunction noStaticIgniteLogger = ArchRuleDefinition.noFields()
                .that().haveRawType(IgniteLogger.class)
                .should().haveModifier(JavaModifier.STATIC);

        noStaticIgniteLogger.check(clientModuleClasses());
        noStaticIgniteLogger.check(clientModuleDependencies());
    }
}
