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

import static com.tngtech.archunit.base.DescribedPredicate.and;
import static com.tngtech.archunit.core.domain.properties.CanBeAnnotated.Predicates.annotatedWith;
import static com.tngtech.archunit.core.importer.ImportOption.Predefined.DO_NOT_INCLUDE_TESTS;
import static com.tngtech.archunit.core.importer.ImportOption.Predefined.DO_NOT_INCLUDE_TEST_FIXTURES;
import static com.tngtech.archunit.lang.SimpleConditionEvent.violated;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;

import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.domain.JavaMethod;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ArchRule;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.syntax.ArchRuleDefinition;
import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.network.annotations.Transferable;
import org.apache.ignite.internal.raft.BaseCommandsCompatibilityTest;
import org.apache.ignite.internal.raft.BaseCommandsCompatibilityTest.TestCommand;
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
        Set<String> raftCommands = toClassNames(collectRaftCommands());

        assertThat(raftCommands, not(empty()));

        ArchRule archRule = ArchRuleDefinition.classes()
                .that()
                .areAssignableTo(BaseCommandsCompatibilityTest.class)
                .and()
                .containAnyMethodsThat(and(
                        annotatedWith(TestCommand.class),
                        annotatedWith(Test.class).or(annotatedWith(ParameterizedTest.class))
                ))
                .should(shouldContainsTestMethodsForRaftCommands(raftCommands));

        archRule.check(classes);

        assertThat(
                "There are still some raft commands that haven't been tested; for example, see the successors of "
                        + BaseCommandsCompatibilityTest.class.getName(),
                raftCommands,
                empty()
        );
    }

    private static ArchCondition<JavaClass> shouldContainsTestMethodsForRaftCommands(Set<String> raftCommands) {
        return new ArchCondition<>("should contain test methods for raft commands") {
            @Override
            public void check(JavaClass item, ConditionEvents events) {
                System.out.println("Checking class: " + item.getName());

                for (JavaMethod method : item.getMethods()) {
                    if (hasTestMethodTestCommand(method)) {
                        String testedRaftCommandClass = method.getAnnotationOfType(TestCommand.class).value().getName();

                        if (!raftCommands.remove(testedRaftCommandClass)) {
                            events.add(violated(
                                    method,
                                    "another method is already testing the command: " + testedRaftCommandClass
                            ));
                        }
                    }
                }
            }
        };
    }

    private static Set<JavaClass> collectRaftCommands() {
        // Uses its own JavaClasses to avoid OutOfMemoryError from a large number of classes.
        JavaClasses classes = new ClassFileImporter()
                .withImportOptions(List.of(DO_NOT_INCLUDE_TESTS, DO_NOT_INCLUDE_TEST_FIXTURES))
                .importPackages("org.apache.ignite");

        JavaClass raftCommandJavaClass = classes.get(Command.class);

        return classes.stream()
                .filter(JavaClass::isInterface)
                .filter(javaClass -> javaClass.isAnnotatedWith(Transferable.class))
                .filter(javaClass -> javaClass.getAllRawInterfaces().contains(raftCommandJavaClass))
                .collect(toSet());
    }

    private static Set<String> toClassNames(Set<JavaClass> classes) {
        return classes.stream().map(JavaClass::getName).collect(toSet());
    }

    private static boolean hasTestMethodTestCommand(JavaMethod method) {
        return method.isAnnotatedWith(TestCommand.class)
                && (method.isAnnotatedWith(Test.class) || method.isAnnotatedWith(ParameterizedTest.class));
    }
}
