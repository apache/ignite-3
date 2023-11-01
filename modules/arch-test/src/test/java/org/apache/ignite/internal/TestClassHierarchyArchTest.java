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

import static com.tngtech.archunit.core.domain.JavaClass.Predicates.containAnyMethodsThat;
import static com.tngtech.archunit.core.domain.properties.CanBeAnnotated.Predicates.annotatedWith;

import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;
import com.tngtech.archunit.lang.syntax.ArchRuleDefinition;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.lang.IgniteTestImportOption;
import org.apache.ignite.lang.LocationProvider.RootLocationProvider;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;

/**
 * Tests that tests classes, which uses Mockito, extends BaseIgniteAbstractTest. Using BaseIgniteAbstractTest guarantees the Mockito
 * resources will be released after tests finishes.
 */
@SuppressWarnings("JUnitTestCaseWithNoTests")
@AnalyzeClasses(
        packages = "org.apache.ignite",
        importOptions = IgniteTestImportOption.class,
        locations = RootLocationProvider.class)
public class TestClassHierarchyArchTest {
    private static final DescribedPredicate<JavaClass> hasMockitoDependency = DescribedPredicate.describe(
            "Depends on Mockito",
            javaClass -> javaClass.getTransitiveDependenciesFromSelf().stream()
                    .anyMatch(dependency -> dependency.getTargetClass().getPackageName().startsWith("org.mockito")));

    @ArchTest
    public static final ArchRule TEST_CLASS_WITH_MOCKS_EXTENDS_BASE_TEST_CLASS = ArchRuleDefinition
            .classes()
            .that()
            // Ignore nested test classes, only their outer classes need to inherit.
            .areNotAnnotatedWith(Nested.class)
            .and(containAnyMethodsThat(annotatedWith(Test.class).or(annotatedWith(ParameterizedTest.class))))
            .and(hasMockitoDependency)
            .should()
            .beAssignableTo(BaseIgniteAbstractTest.class)
            .as("Test classes which use Mockito must extends BaseIgniteAbstractTest.\n"
                    + "This is a workaround for memory leaks in Mockito, see MockitoFramework#clearInlineMocks for details.");
}
