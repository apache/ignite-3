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

package org.apache.ignite.internal.sql.engine.util;

import static java.lang.reflect.Modifier.isStatic;
import static org.apache.ignite.lang.IgniteStringFormatter.format;
import static org.junit.jupiter.api.Assertions.fail;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.commons.support.AnnotationSupport;
import org.junit.platform.commons.support.HierarchyTraversalMode;

/**
 * JUnit extension to inject {@link QueryCheckerFactory} instance into test classes, and ensure the {@link QueryChecker#check()} method is
 * called for each {@link QueryChecker} instance, which was created via the factory.
 *
 * @see InjectQueryCheckerFactory
 */
public class QueryCheckerExtension implements BeforeEachCallback, BeforeAllCallback, AfterEachCallback {
    /** QueryCheckers instances that are managed by this extension. */
    private static final Set<QueryChecker> QUERY_CHECKERS = new HashSet<>();

    private static final QueryCheckerFactory FACTORY_INSTANCE = new QueryCheckerFactoryImpl(
            QueryCheckerExtension::register,
            QueryCheckerExtension::unregister
    );

    /** {@inheritDoc} */
    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        injectFields(context, true);
    }

    /** {@inheritDoc} */
    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        injectFields(context, false);
    }

    /** {@inheritDoc} */
    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        ensureNoUnusedChecker();
    }

    private static void injectFields(ExtensionContext context, boolean forStatic) throws Exception {
        Class<?> testClass = context.getRequiredTestClass();
        Object testInstance = context.getTestInstance().orElse(null);

        assert forStatic || testInstance != null;

        List<Field> annotatedFields = AnnotationSupport.findAnnotatedFields(
                testClass,
                InjectQueryCheckerFactory.class,
                field -> field.getType().isAssignableFrom(QueryCheckerFactory.class) && (isStatic(field.getModifiers()) == forStatic),
                HierarchyTraversalMode.TOP_DOWN
        );

        for (Field field : annotatedFields) {
            field.setAccessible(true);

            field.set(forStatic ? null : testInstance, FACTORY_INSTANCE);
        }
    }

    /**
     * Remembers the given QueryChecker instance.
     */
    private static void register(QueryChecker newChecker) {
        QUERY_CHECKERS.add(newChecker);
    }

    /**
     * Unregisters created QueryChecker instance.
     */
    private static void unregister(QueryChecker queryChecker) {
        boolean remove = QUERY_CHECKERS.remove(queryChecker);

        if (!remove) {
            throw new IllegalStateException(format("Unknown QueryChecker instance for SQL query: {}", queryChecker));
        }
    }

    /**
     * Validates that {@link QueryChecker#check()} was called for each QueryChecker instance, which was created via the factory.
     *
     * @throws AssertionError If found any registered QueryChecker.
     * @see QueryCheckerExtension
     */
    private static void ensureNoUnusedChecker() {
        if (QUERY_CHECKERS.isEmpty()) {
            return;
        }

        String failureDetails = QUERY_CHECKERS.stream()
                .map(Object::toString)
                .collect(Collectors.joining("\n", "Found unused QueryCheckers for queries: ", ""));

        // Clear collection to allow passing next tests in suite.
        QUERY_CHECKERS.clear();

        fail(failureDetails);
    }

}
