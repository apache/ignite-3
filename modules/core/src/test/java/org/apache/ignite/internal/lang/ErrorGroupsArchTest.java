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

package org.apache.ignite.internal.lang;

import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaField;
import com.tngtech.archunit.core.importer.Location;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.junit.LocationProvider;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ArchRule;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;
import com.tngtech.archunit.lang.syntax.ArchRuleDefinition;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Set;
import org.apache.ignite.internal.lang.ErrorGroupsArchTest.CoreLocationProvider;
import org.apache.ignite.lang.ErrorGroups;

/**
 * Test that all error code groups has correct naming.
 */
@AnalyzeClasses(
        packages = "org.apache.ignite.lang",
        locations = CoreLocationProvider.class)
public class ErrorGroupsArchTest {

    static class CoreLocationProvider implements LocationProvider {
        @Override
        public Set<Location> get(Class<?> testClass) {
            // ignite-3/modules/core
            Path modulesRoot = Path.of("").toAbsolutePath();

            return Set.of(Location.of(modulesRoot));
        }
    }

    @SuppressWarnings("unused")
    @ArchTest
    public static final ArchRule ERROR_GROUPS_NAME_CONVENTION = ArchRuleDefinition.classes().that().areNestedClasses()
            .should(new ArchCondition<>("Check name convention for errors") {
                @Override
                public void check(JavaClass javaClass, ConditionEvents conditionEvents) {
                    Optional<JavaClass> enclosingClass = javaClass.getEnclosingClass();
                    if (enclosingClass.isPresent() && enclosingClass.get().getFullName().equals(ErrorGroups.class.getName())) {
                        boolean groupFound = false;
                        for (JavaField field : javaClass.getFields()) {
                            if (field.getType().toErasure().reflect() == ErrorGroups.class) {
                                if (!groupFound) {
                                    groupFound = true;
                                    if (!field.getName().endsWith("GROUP")) {
                                        conditionEvents.add(SimpleConditionEvent.violated(field,
                                                "Group definition field name must end with GROUP suffix"));
                                    }
                                } else {
                                    conditionEvents.add(SimpleConditionEvent.violated(field,
                                            "Each error group class must have only one error group definition."));
                                }
                            }
                            if (field.getRawType().reflect() == int.class) {
                                if (!field.getName().endsWith("ERR")) {
                                    conditionEvents.add(SimpleConditionEvent.violated(field,
                                            "Error code definition field name must end with ERR suffix"));
                                }
                            }
                        }
                    }
                }
            });
}
