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

package org.apache.ignite.lang;

import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaMethod;
import com.tngtech.archunit.core.importer.Location;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.junit.LocationProvider;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ArchRule;
import com.tngtech.archunit.lang.ConditionEvent;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;
import com.tngtech.archunit.lang.syntax.ArchRuleDefinition;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Set;
import org.junit.jupiter.api.Disabled;

/**
 * Tests that all muted test have appropriate tickets.
 */
@AnalyzeClasses(
        packages = "org.apache.ignite",
        importOptions = IgniteTestImportOption.class,
        locations = DisabledTestsHasTicketArchTest.RootLocationProvider.class)
public class DisabledTestsHasTicketArchTest {
    static class RootLocationProvider implements LocationProvider {
        @Override
        public Set<Location> get(Class<?> testClass) {
            // ignite-3/modules
            Path modulesRoot = Path.of("").toAbsolutePath().getParent();

            try {
                Files.walkFileTree(modulesRoot, new FileVisitor<>() {
                    @Override
                    public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                        if (dir.toAbsolutePath().toString().contains("build")) {
                            System.out.println("Classes dir");
                            System.out.println(dir.toAbsolutePath());
                            System.out.println("-------");
                        }
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                        return FileVisitResult.CONTINUE;
                    }
                });
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            return Set.of(Location.of(modulesRoot));
        }
    }

    @ArchTest
    public static final ArchRule DISABLED_TEST_CLASS_HAS_TICKET_MENTION = ArchRuleDefinition
            .classes().that().areAnnotatedWith(Disabled.class)
            .should(new ArchCondition<>("test class have mention of IGNITE ticket") {
                @Override
                public void check(JavaClass javaClass, ConditionEvents events) {
                    checkAnnotation(javaClass.getName(), javaClass.getAnnotationOfType(Disabled.class), events);
                }
            });

    @ArchTest
    public static final ArchRule DISABLED_TEST_METHOD_HAS_TICKET_MENTION = ArchRuleDefinition
            .methods().that().areAnnotatedWith(Disabled.class)
            .should(new ArchCondition<>("test method have mention of IGNITE ticket") {
                @Override
                public void check(JavaMethod javaMethod, ConditionEvents events) {
                    checkAnnotation(javaMethod.getFullName(), javaMethod.getAnnotationOfType(Disabled.class), events);
                }
            });

    static void checkAnnotation(String name, Disabled annotation, ConditionEvents events) {
        String disableReason = annotation.value();

        if (disableReason.toUpperCase().contains("IGNITE-")) {
            return;
        }

        ConditionEvent event = SimpleConditionEvent.violated(
                name,
                name + " disabled but does not have a reference to a ticket");

        events.add(event);
    }
}
