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

import static com.tngtech.archunit.core.importer.ImportOption.Predefined.DO_NOT_INCLUDE_TESTS;
import static com.tngtech.archunit.core.importer.ImportOption.Predefined.DO_NOT_INCLUDE_TEST_FIXTURES;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.domain.JavaMethod;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.network.annotations.Transferable;
import org.apache.ignite.internal.raft.BaseCommandsCompatibilityTest;
import org.apache.ignite.internal.raft.BaseCommandsCompatibilityTest.TestForCommand;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.lang.IgniteTestImportOption;
import org.apache.ignite.lang.LocationProvider.RootLocationProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;

/**
 * Tests that all unit commands are tested for backward compatibility.
 */
@AnalyzeClasses(
        packages = "org.apache.ignite",
        importOptions = IgniteTestImportOption.class,
        locations = RootLocationProvider.class
)
public class RaftCommandsUnitCompatibilityArchTest {
    @ArchTest
    void test(JavaClasses classes) {
        Set<String> raftCommands = collectRaftCommands();

        assertThat(raftCommands, not(empty()));

        Set<String> testedRaftCommands = collectTestedRaftCommands(classes);

        Set<String> notTestedRaftCommands = raftCommands.stream()
                .filter(cmd -> !testedRaftCommands.contains(cmd))
                .collect(toSet());

        assertThat(
                "There are still some raft commands that haven't been tested",
                notTestedRaftCommands,
                is(empty())
        );
    }

    private static Set<String> collectRaftCommands() {
        // Uses its own JavaClasses to avoid OutOfMemoryError from a large number of classes.
        JavaClasses classes = new ClassFileImporter()
                .withImportOptions(List.of(DO_NOT_INCLUDE_TESTS, DO_NOT_INCLUDE_TEST_FIXTURES))
                .importPackages("org.apache.ignite");

        return classes.get(Command.class).getAllSubclasses().stream()
                .filter(JavaClass::isInterface)
                .filter(javaClass -> javaClass.isAnnotatedWith(Transferable.class))
                .map(JavaClass::getName)
                .collect(toSet());
    }

    private static Set<String> collectTestedRaftCommands(JavaClasses classes) {
        return classes.get(BaseCommandsCompatibilityTest.class).getAllSubclasses().stream()
                .flatMap(javaClass -> javaClass.getMethods().stream())
                .filter(RaftCommandsUnitCompatibilityArchTest::hasTestMethodTestCommand)
                .map(method -> method.getAnnotationOfType(TestForCommand.class).value())
                .map(Class::getName)
                .collect(toSet());
    }

    private static boolean hasTestMethodTestCommand(JavaMethod method) {
        return method.isAnnotatedWith(TestForCommand.class)
                && (method.isAnnotatedWith(Test.class) || method.isAnnotatedWith(ParameterizedTest.class));
    }
}
