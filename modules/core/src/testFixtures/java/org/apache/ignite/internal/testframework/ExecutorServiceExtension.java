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

package org.apache.ignite.internal.testframework;

import static java.lang.reflect.Modifier.isStatic;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.thread.ThreadOperation;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.platform.commons.support.AnnotationSupport;
import org.junit.platform.commons.support.HierarchyTraversalMode;

/**
 * JUnit extension for injecting temporary {@link ExecutorService}'s into test classes.
 *
 * @see InjectExecutorService
 */
public class ExecutorServiceExtension implements BeforeAllCallback, AfterAllCallback, BeforeEachCallback, AfterEachCallback,
        ParameterResolver {
    private static final int CPUS = Runtime.getRuntime().availableProcessors();

    private static final Namespace NAMESPACE = Namespace.create(ExecutorServiceExtension.class);

    private static final Set<Class<?>> SUPPORTED_FIELD_TYPES = Set.of(ScheduledExecutorService.class, ExecutorService.class);

    private static final Object STATIC_EXECUTORS_KEY = new Object();

    private static final Object INSTANCE_EXECUTORS_KEY = new Object();

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        injectFields(context, true);
    }

    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        shutdownExecutors(context, true);
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        injectFields(context, false);
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        shutdownExecutors(context, false);
    }

    @Override
    public boolean supportsParameter(
            ParameterContext parameterContext,
            ExtensionContext extensionContext
    ) throws ParameterResolutionException {
        return parameterContext.isAnnotated(InjectExecutorService.class)
                && isFieldTypeSupported(parameterContext.getParameter().getType());
    }

    @Override
    public Object resolveParameter(
            ParameterContext parameterContext,
            ExtensionContext extensionContext
    ) throws ParameterResolutionException {
        if (!supportsParameter(parameterContext, extensionContext)) {
            throw new ParameterResolutionException("Unknown parameter:" + parameterContext.getParameter());
        }

        boolean forStatic = isStatic(parameterContext.getParameter().getDeclaringExecutable().getModifiers());

        List<ExecutorService> executorServices = getOrCreateExecutorServiceListInStore(extensionContext, forStatic);

        InjectExecutorService injectExecutorService = parameterContext.findAnnotation(InjectExecutorService.class).orElse(null);

        assert injectExecutorService != null : parameterContext.getParameter();

        ExecutorService executorService = createExecutorService(
                injectExecutorService,
                parameterContext.getParameter().getName(),
                parameterContext.getParameter().getType(),
                extensionContext.getRequiredTestClass(),
                parameterContext.getParameter().getDeclaringExecutable().getName()
        );

        executorServices.add(executorService);

        return executorService;
    }

    private static void injectFields(ExtensionContext context, boolean forStatic) throws Exception {
        Class<?> testClass = context.getRequiredTestClass();
        Object testInstance = context.getTestInstance().orElse(null);

        List<Field> fields = collectFields(testClass, forStatic);

        if (fields.isEmpty()) {
            return;
        }

        List<ExecutorService> executorServices = getOrCreateExecutorServiceListInStore(context, forStatic);

        for (Field field : fields) {
            checkFieldTypeIsSupported(field);

            ExecutorService executorService = createExecutorService(field);

            executorServices.add(executorService);

            field.setAccessible(true);

            field.set(forStatic ? null : testInstance, executorService);
        }
    }

    private static void shutdownExecutors(ExtensionContext context, boolean forStatic) throws Exception {
        List<ExecutorService> removed = (List<ExecutorService>) context.getStore(NAMESPACE).remove(storeKey(forStatic));

        if (removed == null || removed.isEmpty()) {
            return;
        }

        Stream<AutoCloseable> autoCloseableStream = removed.stream()
                .map(executorService -> () -> IgniteUtils.shutdownAndAwaitTermination(executorService, 10, TimeUnit.SECONDS));

        IgniteUtils.closeAll(autoCloseableStream);
    }

    private static List<Field> collectFields(Class<?> testClass, boolean forStatic) {
        return AnnotationSupport.findAnnotatedFields(
                testClass,
                InjectExecutorService.class,
                field -> isStatic(field.getModifiers()) == forStatic,
                HierarchyTraversalMode.TOP_DOWN
        );
    }

    private static void checkFieldTypeIsSupported(Field field) {
        if (!isFieldTypeSupported(field.getType())) {
            throw new IllegalStateException(
                    String.format("Unsupported field type: [field=%s, supportedFieldTypes=%s]", field, SUPPORTED_FIELD_TYPES)
            );
        }
    }

    private static boolean isFieldTypeSupported(Class<?> fieldType) {
        for (Class<?> supportedFieldType : SUPPORTED_FIELD_TYPES) {
            if (fieldType.equals(supportedFieldType)) {
                return true;
            }
        }

        return false;
    }

    private static ExecutorService createExecutorService(Field field) {
        InjectExecutorService injectExecutorService = field.getAnnotation(InjectExecutorService.class);

        assert injectExecutorService != null : field;

        return createExecutorService(injectExecutorService, field.getName(), field.getType(), field.getDeclaringClass(), null);
    }

    private static ExecutorService createExecutorService(
            InjectExecutorService injectExecutorService,
            String fieldName,
            Class<?> fieldType,
            Class<?> testClass,
            @Nullable String methodName
    ) {
        int threadCount = injectExecutorService.threadCount();
        String threadPrefix = threadPrefix(injectExecutorService, testClass, fieldName, methodName);
        ThreadOperation[] allowedOperations = injectExecutorService.allowedOperations();

        ThreadFactory threadFactory = IgniteThreadFactory.withPrefix(threadPrefix, Loggers.forClass(testClass), allowedOperations);

        if (fieldType.equals(ScheduledExecutorService.class)) {
            return newScheduledThreadPool(threadCount == 0 ? 1 : threadCount, threadFactory);
        } else if (fieldType.equals(ExecutorService.class)) {
            return newFixedThreadPool(threadCount == 0 ? CPUS : threadCount, threadFactory);
        }

        throw new AssertionError(
                String.format("Unsupported field type: [field=%s, supportedFieldTypes=%s]", fieldName, SUPPORTED_FIELD_TYPES)
        );
    }

    private static String threadPrefix(
            InjectExecutorService injectExecutorService,
            Class<?> testClass,
            String fieldName,
            @Nullable String methodName
    ) {
        String threadPrefix = injectExecutorService.threadPrefix();

        if (threadPrefix != null && !"".equals(threadPrefix)) {
            return threadPrefix;
        }

        if (methodName == null) {
            return String.format("test-%s-%s", testClass.getSimpleName(), fieldName);
        }

        return String.format("test-%s-%s-%s", testClass.getSimpleName(), methodName, fieldName);
    }

    private static Object storeKey(boolean forStatic) {
        return forStatic ? STATIC_EXECUTORS_KEY : INSTANCE_EXECUTORS_KEY;
    }

    private static List<ExecutorService> getOrCreateExecutorServiceListInStore(ExtensionContext context, boolean forStatic) {
        List<ExecutorService> executorServices = (List<ExecutorService>) context.getStore(NAMESPACE).get(storeKey(forStatic));

        if (executorServices == null) {
            executorServices = new CopyOnWriteArrayList<>();

            context.getStore(NAMESPACE).put(storeKey(forStatic), executorServices);
        }

        return executorServices;
    }
}
