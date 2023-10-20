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

import static com.tngtech.archunit.core.domain.JavaClass.Predicates.assignableTo;
import static com.tngtech.archunit.lang.conditions.ArchPredicates.are;

import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ArchRule;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;
import com.tngtech.archunit.lang.syntax.ArchRuleDefinition;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.ConfigurationModule;
import org.apache.ignite.configuration.SuperRootChange;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.internal.configuration.ServiceLoaderModulesProvider;
import org.apache.ignite.lang.LocationProvider.RootLocationProvider;

/**
 * Tests that all local configuration modules do not override
 * {@link ConfigurationModule#patchConfigurationWithDynamicDefaults(SuperRootChange)}.
 */
@AnalyzeClasses(
        packages = "org.apache.ignite",
        importOptions = ImportOption.DoNotIncludeTests.class,
        locations = RootLocationProvider.class)
public class ConfigurationModuleArchTest {
    @ArchTest
    public static final ArchRule LOCAL_CONFIGURATION_MODULES_DOES_NOT_OVERRIDE = ArchRuleDefinition.classes()
            .that(new Predicate())
            .and(are(assignableTo(ConfigurationModule.class)))
            .should(new ArchCondition<>(
                    "not override org.apache.ignite.configuration.ConfigurationModule.patchConfigurationWithDynamicDefaults") {
                @Override
                public void check(JavaClass javaClass, ConditionEvents conditionEvents) {
                    boolean overrided = javaClass.getMethods().stream()
                            .anyMatch(it -> it.getName().equals("patchConfigurationWithDynamicDefaults"));

                    if (overrided) {
                        conditionEvents.add(SimpleConditionEvent.violated(
                                javaClass,
                                javaClass.getName() + " overrides patchConfigurationWithDynamicDefaults"
                        ));
                    }
                }
            });

    private static final Set<String> local;

    static {
        var modulesProvider = new ServiceLoaderModulesProvider();
        List<ConfigurationModule> modules = modulesProvider.modules(ConfigurationModuleArchTest.class.getClassLoader());
        local = modules.stream()
                .filter(it -> it.type() == ConfigurationType.LOCAL)
                .map(it -> it.getClass().getCanonicalName())
                .collect(Collectors.toSet());
    }

    private static class Predicate extends DescribedPredicate<JavaClass> {
        public Predicate() {
            super("local configuration module");
        }

        @Override
        public boolean test(JavaClass javaClass) {
            return local.contains(javaClass.getFullName());
        }
    }
}
