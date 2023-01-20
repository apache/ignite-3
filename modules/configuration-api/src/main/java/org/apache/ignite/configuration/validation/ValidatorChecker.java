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

package org.apache.ignite.configuration.validation;

import java.lang.annotation.Annotation;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.WildcardType;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.annotation.Value;

/**
 * Helper class for the default {@link Validator#canValidate(Class, Class, boolean)} implementation.
 */
class ValidatorChecker {
    /**
     * Pattern for configuration schema class names. Used to replace the suffix with something else.
     *
     * @see #canValidateConfigValue(Class, Class)
     */
    private static final Pattern CONFIGURATION_SCHEMA_PATTERN = Pattern.compile("ConfigurationSchema$");

    /**
     * Please refer to {@link Validator#canValidate(Class, Class, boolean)} for parameters descriptions.
     */
    static boolean canValidate(
            Validator<?, ?> validator,
            Class<? extends Annotation> annotationType,
            Class<?> schemaFieldType,
            boolean namedList
    ) {
        Type[] genericTypeParameters = genericTypeParameters(validator);

        // Check that passed annotation matches the annotation from generic type. For example, that
        // "Validator<Foo, ?>" can validate the annotation "Foo.class".
        if (genericTypeParameters[0] != annotationType) {
            return false;
        }

        Type viewType = genericTypeParameters[1];

        if (viewType instanceof ParameterizedType) {
            ParameterizedType parameterizedType = (ParameterizedType) viewType;

            assert parameterizedType.getRawType() == NamedListView.class : "Unsupported value type in validator " + validator.getClass();

            if (!namedList) {
                return false;
            }

            Type namedListElementType = parameterizedType.getActualTypeArguments()[0];

            if (namedListElementType instanceof WildcardType) {
                return true;
            }

            assert namedListElementType instanceof Class : "Unsupported value type in validator " + validator.getClass();

            Class<?> namedListElementClass = (Class<?>) namedListElementType;

            return canValidateConfigValue(schemaFieldType, namedListElementClass);
        }

        if (namedList) {
            return false;
        }

        assert viewType instanceof Class : "Unsupported value type in validator " + validator.getClass();

        if (schemaFieldType.getName().endsWith("ConfigurationSchema")) {
            return canValidateConfigValue(schemaFieldType, (Class<?>) viewType);
        }

        return canValidateValue(schemaFieldType, (Class<?>) viewType);
    }

    /**
     * Extracts real type arguments from {@link Validator} instance. Works in assumption that the validator is not a lambda and that it
     * defines both actual types in a single {@code extends Validator<Foo, Bar>} inheritance.
     */
    private static Type[] genericTypeParameters(Validator<?, ?> validator) {
        // Find superclass that directly extends Validator.
        var theClass = (Class<? extends Validator<?, ?>>) validator.getClass();

        while (!List.of(theClass.getInterfaces()).contains(Validator.class)) {
            theClass = (Class<? extends Validator<?, ?>>) theClass.getSuperclass();
        }

        // Find generic interface that represents Validator with its generic type parameters.
        Type genericInterface = Arrays.stream(theClass.getGenericInterfaces())
                .filter(i -> i instanceof ParameterizedType && ((ParameterizedType) i).getRawType() == Validator.class)
                .findAny()
                .orElse(null);

        // Must be there, otherwise what are we doing here?
        assert genericInterface != null : validator;

        ParameterizedType parameterizedInterface = (ParameterizedType) genericInterface;

        return parameterizedInterface.getActualTypeArguments();
    }

    /**
     * Checks whether the "view" instance for the configuration property, described by the {@code schemaFieldType} in the schema, can be
     * assigned to a variable of type {@code viewClass}, assuming that schema field is annotated with {@link Value}.
     */
    private static boolean canValidateValue(Class<?> schemaFieldType, Class<?> viewClass) {
        // All primitives must be boxed. Not primitive types here could be: array, String, UUID.
        if (schemaFieldType.isPrimitive()) {
            switch (schemaFieldType.getSimpleName()) {
                case "boolean":
                    schemaFieldType = Boolean.class;
                    break;

                case "byte":
                    schemaFieldType = Byte.class;
                    break;

                case "short":
                    schemaFieldType = Short.class;
                    break;

                case "int":
                    schemaFieldType = Integer.class;
                    break;

                case "long":
                    schemaFieldType = Long.class;
                    break;

                case "float":
                    schemaFieldType = Float.class;
                    break;

                case "double":
                    schemaFieldType = Double.class;
                    break;

                case "char":
                    schemaFieldType = Character.class;
                    break;

                default:
                    // No-op.
            }
        }

        return viewClass.isAssignableFrom(schemaFieldType);
    }

    /**
     * Checks whether the "view" instance for the configuration property, described by the {@code schemaFieldType} in the schema, can be
     * assigned to a variable of type {@code viewClass}, assuming that {@code schemaFieldType} is a configuration schema type itself.
     */
    private static boolean canValidateConfigValue(Class<?> schemaFieldType, Class<?> viewClass) {
        if (viewClass == Object.class) {
            return true;
        }

        // Here the matching itself is kind of tricky. Since we can't get view class instance by the configuration schema class instance,
        // we implement a workaround and match names.
        String viewClassName = viewClass.getName();

        // View class must actually be a view class, that's the restriction. It can be loosen, but there's no reason for it right now.
        assert viewClassName.endsWith("View") : viewClassName;

        // Since configuration schemas can be inherited, we must check all superclasses.
        while (true) {
            // Stop when we hit java.lang.Object.
            if (!schemaFieldType.getSimpleName().endsWith("ConfigurationSchema")) {
                return false;
            }

            // Here we have a copy-pasted algorithm for generating view class name from configuration schema class name.
            // Such weird approach allows us to safely handle inner configuration schema classes, there's plenty of them in tests.
            String schemaFieldViewTypeName = schemaFieldType.getPackageName() + "."
                    + CONFIGURATION_SCHEMA_PATTERN.matcher(schemaFieldType.getSimpleName()).replaceAll("View");

            if (schemaFieldViewTypeName.equals(viewClassName)) {
                return true;
            }

            schemaFieldType = schemaFieldType.getSuperclass();
        }
    }
}
