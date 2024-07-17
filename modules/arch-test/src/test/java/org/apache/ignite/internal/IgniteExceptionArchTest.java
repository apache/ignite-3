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
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.client.IgniteClientConnectionException;
import org.apache.ignite.client.IgniteClientFeatureNotSupportedByServerException;
import org.apache.ignite.internal.network.RecipientLeftException;
import org.apache.ignite.internal.network.UnresolvableConsistentIdException;
import org.apache.ignite.lang.CursorClosedException;
import org.apache.ignite.lang.IgniteCheckedException;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.LocationProvider.RootLocationProvider;
import org.apache.ignite.security.exception.InvalidCredentialsException;
import org.apache.ignite.security.exception.UnsupportedAuthenticationTypeException;
import org.apache.ignite.sql.NoRowSetExpectedException;

/**
 * Tests that all public Ignite exceptions have correct definitions.
 */
@AnalyzeClasses(
        packages = "org.apache.ignite",
        importOptions = ImportOption.DoNotIncludeTests.class,
        locations = RootLocationProvider.class)
public class IgniteExceptionArchTest {
    @ArchTest
    public static final ArchRule IGNITE_EXCEPTIONS_HAVE_REQUIRED_CONSTRUCTORS = ArchRuleDefinition.classes()
            .that(new ExclusionPredicate())
            .and(are(assignableTo(IgniteException.class)).or(assignableTo(IgniteCheckedException.class)))
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

    private static final Set<String> exclusions = new HashSet<>();

    static {
        exclusions.add(IgniteClientConnectionException.class.getCanonicalName());
        exclusions.add(IgniteClientFeatureNotSupportedByServerException.class.getCanonicalName());
        exclusions.add(UnresolvableConsistentIdException.class.getCanonicalName());
        exclusions.add(CursorClosedException.class.getCanonicalName());
        exclusions.add(NoRowSetExpectedException.class.getCanonicalName());
        exclusions.add(InvalidCredentialsException.class.getCanonicalName());
        exclusions.add(UnsupportedAuthenticationTypeException.class.getCanonicalName());
        exclusions.add(RecipientLeftException.class.getCanonicalName());
    }

    private static class ExclusionPredicate extends DescribedPredicate<JavaClass> {
        public ExclusionPredicate() {
            super("Exclusion list");
        }

        @Override
        public boolean test(JavaClass javaClass) {
            return !exclusions.contains(javaClass.getFullName());
        }
    }
}
