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
import com.tngtech.archunit.core.domain.JavaMethod;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ArchRule;
import com.tngtech.archunit.lang.ConditionEvent;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;
import com.tngtech.archunit.lang.syntax.ArchRuleDefinition;
import org.apache.ignite.lang.IgniteTestImportOption;
import org.apache.ignite.lang.LocationProvider.RootLocationProvider;
import org.junit.jupiter.api.Disabled;

/**
 * Tests that all muted test have appropriate tickets.
 */
@AnalyzeClasses(
        packages = "org.apache.ignite",
        importOptions = IgniteTestImportOption.class,
        locations = RootLocationProvider.class)
public class DisabledTestsHasTicketArchTest {
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

        if (disableReason != null && disableReason.toUpperCase().contains("IGNITE-")) {
            return;
        }

        ConditionEvent event = SimpleConditionEvent.violated(
                name,
                name + " disabled but does not have a reference to a ticket");

        events.add(event);
    }
}
