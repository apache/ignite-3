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
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.configuration.ConfigurationModule;
import org.apache.ignite.configuration.annotation.AbstractConfiguration;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.ConfigurationExtension;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.InjectedName;
import org.apache.ignite.configuration.annotation.InjectedValue;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.PolymorphicConfig;
import org.apache.ignite.configuration.annotation.PolymorphicId;
import org.apache.ignite.configuration.annotation.PublicName;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.internal.configuration.compatibility.framework.ConfigNode.Attributes;
import org.apache.ignite.internal.configuration.compatibility.framework.ConfigNode.Flags;
import org.apache.ignite.internal.configuration.util.ConfigurationUtil;

/*
 TODO: https://issues.apache.org/jira/browse/IGNITE-25571
   support named lists. See {@link org.apache.ignite.configuration.annotation.NamedConfigValue} annotation.
 TODO: https://issues.apache.org/jira/browse/IGNITE-25572
   support polymorphic nodes. See {@link org.apache.ignite.configuration.annotation.PolymorphicConfig} annotation.
*/

/**
 * Provides method to extract metadata from project configuration classes.
 */
public class ConfigurationTreeScanner {
    private static final Set<Class<?>> SUPPORTED_ANNOTATIONS = Set.of(
            Value.class,
            Deprecated.class,
            NamedConfigValue.class,
            PublicName.class,
            PolymorphicId.class,
            InjectedName.class,
            InjectedValue.class
    );

    /**
     * Scans the given configuration class and populates the configuration tree structure.
     *
     * @param currentNode The current node in the configuration tree.
     * @param schemaClass The configuration schema class to scan.
     * @param context The context containing dependency information.
     */
    public static void scan(ConfigNode currentNode, Class<?> schemaClass, ScanContext context) {
        assert schemaClass != null && schemaClass.getName().startsWith("org.apache.ignite");

        Collection<Class<?>> extensions = context.getExtensions(schemaClass);

        if (!extensions.isEmpty()) {
            extensions.stream()
                    .sorted(Comparator.comparing(Class::getName)) // Sort for test stability.
                    .forEach(ext -> scan(currentNode, ext, context));

            return;
        }

        List<ConfigNode> children = new ArrayList<>();
        configurationClasses(schemaClass).stream()
                .flatMap(c -> Arrays.stream(c.getDeclaredFields()))
                .filter(field -> !Modifier.isStatic(field.getModifiers()))
                .sorted(Comparator.comparing(Field::getName)) // Sort for test stability.
                .forEach(field -> {
                    ConfigNode node = createNodeForField(currentNode, field);

                    children.add(node);
                    if (!node.isValue()) {
                        scan(node, field.getType(), context);
                    }
                });

        currentNode.addChildNodes(children);
    }

    private static List<Class<?>> configurationClasses(Class<?> configClass) {
        List<Class<?>> classes = new ArrayList<>();
        Class<?> current = configClass;
        while (current != Object.class) {
            assert current.isAnnotationPresent(Config.class)
                    || current.isAnnotationPresent(ConfigurationRoot.class)
                    || current.isAnnotationPresent(PolymorphicConfig.class)
                    || current.isAnnotationPresent(ConfigurationExtension.class)
                    || current.isAnnotationPresent(AbstractConfiguration.class) : current;

            classes.add(current);

            current = current.getSuperclass();
        }

        return classes;
    }

    /**
     * Collects annotations that are not supported.
     */
    private static List<ConfigAnnotation> collectAdditionalAnnotations(Field field) {
        return Arrays.stream(field.getDeclaredAnnotations())
                .flatMap(a -> {
                    if (SUPPORTED_ANNOTATIONS.contains(a.annotationType())) {
                        return Stream.empty();
                    } else {
                        ConfigAnnotation configAnnotation = extractAnnotation(a.annotationType().getName(), a);
                        return Stream.of(configAnnotation);
                    }
                })
                .collect(Collectors.toList());
    }

    private static ConfigNode createNodeForField(ConfigNode parent, Field field) {
        List<ConfigAnnotation> annotations = collectAdditionalAnnotations(field);

        EnumSet<ConfigNode.Flags> flags = extractFlags(field);
        Set<String> legacyNames = extractLegacyNames(field);
        String publicProperty = extractPublicPropertyName(field);

        Map<String, String> attributes = new LinkedHashMap<>();
        attributes.put(Attributes.NAME, publicProperty);
        attributes.put(Attributes.CLASS, field.getType().getCanonicalName());

        return new ConfigNode(parent, attributes, annotations, flags, legacyNames, List.of());
    }

    private static Set<String> extractLegacyNames(Field field) {
        if (field.isAnnotationPresent(PublicName.class)) {
            PublicName[] annotation = field.getAnnotationsByType(PublicName.class);

            assert annotation.length == 1;

            return Set.of(annotation[0].legacyNames());
        }

        return Set.of();
    }

    private static String extractPublicPropertyName(Field field) {
        if (field.isAnnotationPresent(PublicName.class)) {
            PublicName[] annotation = field.getAnnotationsByType(PublicName.class);

            assert annotation.length == 1;

            String publicName = annotation[0].value();

            return publicName.isEmpty() ? field.getName() : publicName;
        }

        return field.getName();
    }

    private static EnumSet<ConfigNode.Flags> extractFlags(Field field) {
        EnumSet<ConfigNode.Flags> flags = EnumSet.noneOf(ConfigNode.Flags.class);

        if (!field.isAnnotationPresent(NamedConfigValue.class)
                && !field.isAnnotationPresent(ConfigValue.class)) {
            flags.add(Flags.IS_VALUE);
        }

        if (field.isAnnotationPresent(Deprecated.class)) {
            flags.add(Flags.IS_DEPRECATED);
        }

        return flags;
    }

    /**
     * Context holder contains required metadata to resolve dependencies between configuration classes.
     */
    public static class ScanContext {
        /**
         * Factory method to create a new scan context based on the provided configuration module.
         */
        public static ScanContext create(ConfigurationModule module) {
            return new ScanContext(module.schemaExtensions(), module.polymorphicSchemaExtensions());
        }

        private final Map<Class<?>, Set<Class<?>>> extensions;
        private final Map<Class<?>, Set<Class<?>>> polymorphicExtensions;

        ScanContext(Collection<Class<?>> extensions, Collection<Class<?>> polymorphicExtensions) {
            this.extensions = ConfigurationUtil.schemaExtensions(extensions);
            this.polymorphicExtensions = ConfigurationUtil.polymorphicSchemaExtensions(polymorphicExtensions);
        }

        /**
         * Returns the set of extension classes for the given extended class.
         *
         * @param extendedClass The class whose extensions should be retrieved.
         */
        public Set<Class<?>> getExtensions(Class<?> extendedClass) {
            return extensions.getOrDefault(extendedClass, Set.of());
        }

        /**
         * Returns the set of polymorphic instance classes for the given polymorphic class.
         *
         * @param polymorphicClass The polymorphic class whose implementations instances should be retrieved.
         */
        public Set<Class<?>> getPolymorphicInstances(Class<?> polymorphicClass) {
            return polymorphicExtensions.getOrDefault(polymorphicClass, Set.of());
        }
    }

    /** Creates {@link ConfigAnnotation} from the given java annotation. */
    public static ConfigAnnotation extractAnnotation(String name, Annotation annotation) {
        Class<?> type = annotation.annotationType();
        Repeatable repeatable = type.getAnnotation(Repeatable.class);
        if (repeatable != null) {
            throw new IllegalStateException("Repeatable annotations are not supported: " + annotation);
        }

        Map<String, ConfigAnnotationValue> properties = new HashMap<>();

        for (Method method : type.getMethods()) {
            // Skip methods inherited from the object class such as equals, hashCode, etc.
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
                throw new IllegalStateException("Failed invoke annotation method: " + method, e);
            }

            if (returnType.isArray()) {
                Class<?> componentType = returnType.getComponentType();

                List<Object> elements = convertArray(result, componentType, annotation);
                propertyValue = ConfigAnnotationValue.createArray(componentType.getName(), elements);
            } else {
                Object convertedValue = convertValue(result, returnType, annotation);
                propertyValue = ConfigAnnotationValue.createValue(returnType.getName(), convertedValue);
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
        if (returnType == byte.class || returnType == short.class || returnType == int.class || returnType == long.class) {
            // Store integer types as longs because jackson deserializes longs that fit into INT as ints by default,
            // it is to store read ints as longs to make validation easier.
            Number val = (Number) value;
            return val.longValue();
        } else if (value instanceof Float || value instanceof Double) {
            return value;
        } else if (returnType == String.class || returnType == boolean.class) {
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
        private @interface EmptyAnnotation {
        }

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

