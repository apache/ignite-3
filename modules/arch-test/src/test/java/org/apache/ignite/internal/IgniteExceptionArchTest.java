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

import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaConstructor;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ArchRule;
import com.tngtech.archunit.lang.ConditionEvent;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;
import com.tngtech.archunit.lang.syntax.ArchRuleDefinition;
import java.util.Optional;
import java.util.UUID;
import org.apache.ignite.lang.IgniteCheckedException;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.LocationProvider.RootLocationProvider;

/**
 * Tests that all public Ignite exceptions have correct definitions.
 */
@AnalyzeClasses(
        packages = "org.apache.ignite",
        importOptions = ImportOption.DoNotIncludeTests.class,
        locations = RootLocationProvider.class)
public class IgniteExceptionArchTest {
    @SuppressWarnings("unused")
    @ArchTest
    public static final ArchRule IGNITE_EXCEPTIONS_HAVE_REQUIRED_CONSTRUCTORS = ArchRuleDefinition.classes()
            .that().areAssignableTo(IgniteException.class)
            .or().areAssignableTo(IgniteCheckedException.class)
            .should(new ArchCondition<>("have standard IgniteException constructor") {
                @Override
                public void check(JavaClass javaClass, ConditionEvents conditionEvents) {
                    if (javaClass.getName().contains(".internal.")) {
                        return;
                    }

                    Optional<JavaConstructor> ctor = javaClass.tryGetConstructor(UUID.class, int.class, String.class, Throwable.class);

                    if (!ctor.isPresent()) {
                        ConditionEvent event = SimpleConditionEvent.violated(
                                javaClass,
                                javaClass.getName() + " does not have a standard constructor with "
                                        + "(UUID traceId, int code, String message, Throwable cause) signature.");

                        conditionEvents.add(event);
                    }
                }
            });
}
