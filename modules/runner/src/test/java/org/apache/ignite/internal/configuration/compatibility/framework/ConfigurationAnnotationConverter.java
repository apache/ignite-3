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

package org.apache.ignite.internal.configuration.compatibility.framework;

import java.lang.annotation.Annotation;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

final class ConfigurationAnnotationConverter {

    static ConfigAnnotation convert(String name, Annotation annotation) {
        Class<?> type = annotation.annotationType();
        Repeatable repeatable = type.getAnnotation(Repeatable.class);
        if (repeatable != null) {
            throw new IllegalStateException("Repeatable annotations are not supported: " + annotation);
        }

        Map<String, ConfigAnnotationValue> properties = new HashMap<>();

        for (Method method : type.getMethods()) {
            if (BuiltinMethod.METHODS.contains(new BuiltinMethod(method))) {
                continue;
            }

            String propertyName = method.getName();
            Class<?> returnType = method.getReturnType();
            ConfigAnnotationValue propertyValue;

            Object result;
            try {
                result = method.invoke(annotation);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new IllegalStateException("Unable read annotation method: " + method, e);
            }

            if (returnType.isArray()) {
                Class<?> componentType = returnType.getComponentType();

                List<Object> elements = convertArray(result, componentType, annotation);
                propertyValue = ConfigAnnotationValue.array(componentType.getName(), elements);
            } else {
                Object convertedValue = convertValue(result, returnType, annotation);
                propertyValue = ConfigAnnotationValue.value(returnType.getName(), convertedValue);
            }

            properties.put(propertyName, propertyValue);
        }

        return new ConfigAnnotation(name, properties);
    }

    private static <T> List<Object> convertArray(Object elements, Class<?> elementType, Annotation annotation) {
        int length = Array.getLength(elements);
        List<Object> list = new ArrayList<>(length);

        for (int i = 0; i < length; i++) {
            Object element = Array.get(elements, i);
            Object convertedElement = convertValue(element, elementType, annotation);

            list.add(convertedElement);
        }

        return list;
    }

    private static Object convertValue(Object value, Class<?> returnType, Annotation annotation) {
        if (returnType.isPrimitive() || returnType == String.class) {
            return value;
        } else if (returnType.isEnum()) {
            return value.toString();
        } else if (returnType == Class.class) {
            Class<?> clazz = (Class<?>) value;
            return clazz.getName();
        } else {
            throw new IllegalArgumentException("Supported annotation property type: " + returnType + ". Annotation: " + annotation);
        }
    }

    private static final class BuiltinMethod {

        @Retention(RetentionPolicy.RUNTIME)
        private @interface EmptyAnnotation { }

        private static final Set<BuiltinMethod> METHODS;

        static {
            // Collect methods that are present on all annotation classes, so we can exclude them from processing.
            METHODS = Arrays.stream(EmptyAnnotation.class.getMethods())
                    .map(BuiltinMethod::new)
                    .collect(Collectors.toSet());
        }

        private final String name;

        private final Class<?> returnType;

        private final List<Object> parameterTypes;

        private BuiltinMethod(Method method) {
            this.name = method.getName();
            this.returnType = method.getReturnType();
            this.parameterTypes = Arrays.asList(method.getParameterTypes());
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            BuiltinMethod that = (BuiltinMethod) o;
            return Objects.equals(name, that.name) && Objects.equals(returnType, that.returnType) && Objects.equals(
                    parameterTypes, that.parameterTypes);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, returnType, parameterTypes);
        }
    }
}
