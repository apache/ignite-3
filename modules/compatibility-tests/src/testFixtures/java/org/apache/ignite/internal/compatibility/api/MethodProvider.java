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

package org.apache.ignite.internal.compatibility.api;

import static java.lang.String.format;
import static org.junit.platform.commons.util.CollectionUtils.isConvertibleToStream;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.commons.util.ClassLoaderUtils;
import org.junit.platform.commons.util.CollectionUtils;
import org.junit.platform.commons.util.Preconditions;
import org.junit.platform.commons.util.ReflectionUtils;

/**
 * Simplified version of {@link org.junit.jupiter.params.provider.MethodArgumentsProvider}.
 */
class MethodProvider {
    private static final Predicate<Method> isFactoryMethod =
            method -> isConvertibleToStream(method.getReturnType());

    static <T> List<T> provideArguments(ExtensionContext context, String methodName) {
        Class<?> testClass = context.getRequiredTestClass();
        Optional<Method> testMethod = context.getTestMethod();
        Object testInstance = context.getTestInstance().orElse(null);

        return Stream.of(methodName)
                .map(factoryMethodName -> findFactoryMethod(testClass, testMethod, factoryMethodName))
                .map(factoryMethod -> context.getExecutableInvoker().invoke(factoryMethod, testInstance))
                .flatMap(CollectionUtils::toStream)
                .map(o -> (T) o)
                .collect(Collectors.toList());
    }

    static boolean looksLikeMethodName(String factoryMethodName) {
        if (factoryMethodName.contains("#")) {
            return true;
        }
        if (factoryMethodName.matches("^[a-zA-Z0-9_]*$")) {
            return true;
        }
        return false;
    }

    private static Method findFactoryMethod(Class<?> testClass, Optional<Method> testMethod, String factoryMethodName) {
        String originalFactoryMethodName = factoryMethodName;

        // Convert local factory method name to fully qualified method name.
        if (looksLikeMethodName(factoryMethodName) && !factoryMethodName.contains("#")) {
            factoryMethodName = testClass.getName() + "#" + factoryMethodName;
        }

        Method factoryMethod = findFactoryMethodByFullyQualifiedName(testClass, testMethod, factoryMethodName);

        Preconditions.condition(isFactoryMethod.test(factoryMethod), () -> format(
                "Could not find valid factory method [%s] for test class [%s] but found the following invalid candidate: %s",
                originalFactoryMethodName, testClass.getName(), factoryMethod));

        return factoryMethod;
    }

    private static @Nullable Method findFactoryMethodByFullyQualifiedName(
            Class<?> testClass,
            Optional<Method> testMethod,
            String fullyQualifiedMethodName
    ) {
        String[] methodParts = ReflectionUtils.parseFullyQualifiedMethodName(fullyQualifiedMethodName);
        String className = methodParts[0];
        String methodName = methodParts[1];
        String methodParameters = methodParts[2];
        ClassLoader classLoader = ClassLoaderUtils.getClassLoader(testClass);
        Class<?> clazz = ReflectionUtils.loadRequiredClass(className, classLoader);

        // Attempt to find an exact match first.
        Method factoryMethod = ReflectionUtils.findMethod(clazz, methodName, methodParameters).orElse(null);

        if (factoryMethod != null) {
            return factoryMethod;
        }

        return null;
    }
}
