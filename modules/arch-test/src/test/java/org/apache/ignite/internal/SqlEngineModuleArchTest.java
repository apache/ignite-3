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
import com.tngtech.archunit.core.domain.JavaModifier;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ArchRule;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;
import com.tngtech.archunit.lang.syntax.ArchRuleDefinition;
import org.apache.calcite.rel.RelNode;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.lang.LocationProvider.RootLocationProvider;

/**
 * Checks that all non-abstract descendants of {@link IgniteRel} override {@link RelNode#getRelTypeName()}.
 */
@AnalyzeClasses(
        packages = "org.apache.ignite.internal.sql.engine.rel",
        importOptions = ImportOption.DoNotIncludeTests.class,
        locations = RootLocationProvider.class)
public class SqlEngineModuleArchTest {
    @ArchTest
    public static final ArchRule ALL_REL_NODES_DEFINE_REL_TYPE_NAME = ArchRuleDefinition.classes()
            .that()
            .implement(IgniteRel.class)
            .and()
            .doNotHaveModifier(JavaModifier.ABSTRACT)
            .should(new ArchCondition<>("override org.apache.calcite.rel.RelNode.getRelTypeName") {
                @Override
                public void check(JavaClass javaClass, ConditionEvents conditionEvents) {
                    boolean overrided = javaClass.getMethods().stream()
                            .anyMatch(it -> "getRelTypeName".equals(it.getName()));

                    if (!overrided) {
                        conditionEvents.add(SimpleConditionEvent.violated(
                                javaClass,
                                javaClass.getName() + " must override method 'getRelTypeName()'"
                        ));
                    }
                }
            });
}
