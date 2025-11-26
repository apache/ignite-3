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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.annotation.Annotation;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Stream;
import org.apache.ignite.internal.configuration.compatibility.framework.ConfigNode.Flags;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests case for {@link ConfigAnnotationsValidator}.
 */
public class ConfigurationAnnotationValidatorSelfTest extends BaseIgniteAbstractTest {

    @Test
    public void addingAnnotationBreaksCompatibility() {
        ConfigNode candidate = newNode(new ConfigAnnotation("a", Map.of()));
        ConfigNode current = newNode(new ConfigAnnotation("a", Map.of()), new ConfigAnnotation("b", Map.of()));

        List<String> errors = new ArrayList<>();
        ConfigAnnotationsValidator validator = new ConfigAnnotationsValidator(Map.of());

        validator.validate(candidate, current, errors);

        assertEquals(List.of("Adding annotations is not allowed. New annotations: [b]"), errors);
    }

    @Test
    public void removingAnnotationBreaksCompatibility() {
        ConfigNode candidate = newNode(new ConfigAnnotation("a", Map.of()), new ConfigAnnotation("b", Map.of()));
        ConfigNode current = newNode(new ConfigAnnotation("a", Map.of()));

        ConfigAnnotationsValidator validator = new ConfigAnnotationsValidator(Map.of());

        List<String> errors = new ArrayList<>();
        validator.validate(candidate, current, errors);

        assertEquals(List.of("Removing annotations is not allowed. Removed annotations: [b]"), errors);
    }

    @Test
    public void testConvertAnnotation() {
        class SomeClass {
            @AllTypes
            @SuppressWarnings("unused")
            public int allTypes;
        }

        ConfigAnnotation annotation = getAnnotation(SomeClass.class, "allTypes", AllTypes.class.getName(), AllTypes.class);
        assertEquals(AllTypes.class.getName(), annotation.name());

        Map<String, Object> expected = new HashMap<>(Map.of(
                "bool", true,
                "int8", 8L, // store integer types as longs
                "int16", 16L, 
                "int32", 32L,
                "int64", 64L,
                "f32", 32.0f,
                "f64", 64.0d,
                "string", "str",
                "enumElement", Abc.B.name(),
                "classElement", Integer.class.getName()
        ));

        for (var e : new HashMap<>(expected).entrySet()) {
            expected.put(e.getKey() + "s", List.of(e.getValue()));
        }

        Map<String, Object> actual = new HashMap<>();
        for (String p : annotation.properties().keySet()) {
            ConfigAnnotationValue value = annotation.properties().get(p);
            actual.put(p, value.value());
        }

        assertEquals(new TreeMap<>(expected), new TreeMap<>(actual));
    }

    @Retention(RetentionPolicy.RUNTIME)
    private @interface AllTypes {
        @SuppressWarnings("unused")
        boolean bool() default true;

        @SuppressWarnings("unused")
        byte int8() default 8;

        @SuppressWarnings("unused")
        short int16() default 16;

        @SuppressWarnings("unused")
        int int32() default 32;

        @SuppressWarnings("unused")
        long int64() default 64;

        @SuppressWarnings("unused")
        float f32() default 32.0f;

        @SuppressWarnings("unused")
        double f64() default 64.0d;

        @SuppressWarnings("unused")
        String string() default "str";

        @SuppressWarnings("unused")
        Abc enumElement() default Abc.B;

        @SuppressWarnings("unused")
        Class<?> classElement() default Integer.class;

        // Arrays

        @SuppressWarnings("unused")
        boolean[] bools() default true;

        @SuppressWarnings("unused")
        byte[] int8s() default 8;

        @SuppressWarnings("unused")
        short[] int16s() default 16;

        @SuppressWarnings("unused")
        int[] int32s() default 32;

        @SuppressWarnings("unused")
        long[] int64s() default 64;

        @SuppressWarnings("unused")
        float[] f32s() default 32.0f;

        @SuppressWarnings("unused")
        double[] f64s() default 64.0d;

        @SuppressWarnings("unused")
        String[] strings() default "str";

        @SuppressWarnings("unused")
        Abc[] enumElements() default Abc.B;

        @SuppressWarnings("unused")
        Class<?>[] classElements() default Integer.class;
    }

    private enum Abc {
        A, B, C
    }

    @ParameterizedTest
    @MethodSource("annotationValidationRules")
    public void basicAnnotationValidationRules(AnnotationValidationRuleArgs args) {
        class SomeClass {
            @Base
            @BasePlusField
            @BaseArray
            @BaseTypeChange
            @BaseArrayTypeChange
            @BaseMultipleChanges(f3 = "")
            @SuppressWarnings("unused")
            public String f1;
        }

        ConfigAnnotation candidate = getAnnotation(SomeClass.class, "f1", "AnnotationType", args.ann1);
        ConfigAnnotation current = getAnnotation(SomeClass.class, "f1", "AnnotationType", args.ann2);

        ConfigAnnotationsValidator validator = new ConfigAnnotationsValidator(Map.of("AnnotationType",
                (candidate1, current1, errors) -> {
                }));

        List<String> errors = new ArrayList<>();
        validator.validate(newNode(candidate), newNode(current), errors);

        expectErrors(args.errors, errors);
    }

    private static Stream<AnnotationValidationRuleArgs> annotationValidationRules() {
        return Stream.of(
                // No changes
                new AnnotationValidationRuleArgs(Base.class, Base.class, Set.of()),

                // Adding field
                new AnnotationValidationRuleArgs(Base.class, BasePlusField.class,
                        Set.of("AnnotationType added properties [f2]")),

                // Removing field 
                new AnnotationValidationRuleArgs(BasePlusField.class, Base.class,
                        Set.of("AnnotationType removed properties [f2]")),

                // Changing field type
                new AnnotationValidationRuleArgs(Base.class, BaseTypeChange.class,
                        Set.of("AnnotationType properties with changed types [f1]")),

                // Changing array field type
                new AnnotationValidationRuleArgs(BaseArray.class, BaseArrayTypeChange.class,
                        Set.of("AnnotationType properties with changed types [f1]")),

                // Multiple errors

                new AnnotationValidationRuleArgs(BasePlusField.class, BaseMultipleChanges.class,
                        Set.of(
                                "AnnotationType properties with changed types [f1]",
                                "AnnotationType added properties [f3]",
                                "AnnotationType removed properties [f2]"
                        )
                )
        );
    }

    private static class AnnotationValidationRuleArgs {
        final Class<? extends Annotation> ann1;

        final Class<? extends Annotation> ann2;

        final Set<String> errors;

        AnnotationValidationRuleArgs(
                Class<? extends Annotation> ann1,
                Class<? extends Annotation> ann2,
                Set<String> errors) {
            this.ann1 = ann1;
            this.ann2 = ann2;
            this.errors = errors;
        }

        @Override
        public String toString() {
            return ann1.getSimpleName() + " -> " + ann2.getSimpleName() + " : " + errors;
        }
    }

    @Retention(RetentionPolicy.RUNTIME)
    @interface Base {
        @SuppressWarnings("unused")
        String f1() default "";
    }

    @Retention(RetentionPolicy.RUNTIME)
    @interface BaseTypeChange {
        @SuppressWarnings("unused")
        int f1() default 0;
    }

    @Retention(RetentionPolicy.RUNTIME)
    @interface BaseArray {
        @SuppressWarnings("unused")
        int[] f1() default 0;
    }

    @Retention(RetentionPolicy.RUNTIME)
    @interface BaseArrayTypeChange {
        @SuppressWarnings("unused")
        boolean[] f1() default {true};
    }

    @Retention(RetentionPolicy.RUNTIME)
    @interface BasePlusField {
        @SuppressWarnings("unused")
        String f1() default "";

        @SuppressWarnings("unused")
        String f2() default "";
    }

    @Retention(RetentionPolicy.RUNTIME)
    @interface BaseMultipleChanges {
        @SuppressWarnings("unused")
        int f1() default 0;

        @SuppressWarnings("unused")
        String f3();
    }

    @ParameterizedTest
    @MethodSource("noValidationRules")
    public void noValidator(AnnotationValidationRuleArgs args) {
        class SomeClass {
            @Base
            @BaseValueChange
            @SuppressWarnings("unused")
            public String f1;
        }

        ConfigAnnotation candidate = getAnnotation(SomeClass.class, "f1", "AnnotationType", args.ann1);
        ConfigAnnotation current = getAnnotation(SomeClass.class, "f1", "AnnotationType", args.ann2);

        ConfigAnnotationsValidator validator = new ConfigAnnotationsValidator(Map.of());

        List<String> errors = new ArrayList<>();
        validator.validate(newNode(candidate), newNode(current), errors);

        expectErrors(args.errors, errors);
    }

    private static Stream<AnnotationValidationRuleArgs> noValidationRules() {
        return Stream.of(
                // No changes
                new AnnotationValidationRuleArgs(Base.class, Base.class, Set.of()),
                // Value changes
                new AnnotationValidationRuleArgs(Base.class, BaseValueChange.class,
                        Set.of(
                                "Annotation requires a custom compatibility validator: AnnotationType",
                                "AnnotationType changed values [f1]"
                        )
                )
        );
    }

    @Retention(RetentionPolicy.RUNTIME)
    @interface BaseValueChange {
        @SuppressWarnings("unused")
        String f1() default "x";
    }

    @Test
    public void callsSpecificValidator() {
        class SomeClass {
            @InvalidAnnotation
            @SuppressWarnings("unused")
            public String f1;
        }

        ConfigAnnotation candidate = getAnnotation(SomeClass.class, "f1", InvalidAnnotation.class.getName(), InvalidAnnotation.class);
        ConfigAnnotation current = getAnnotation(SomeClass.class, "f1", InvalidAnnotation.class.getName(), InvalidAnnotation.class);

        Map<String, AnnotationCompatibilityValidator> validators = Map.of(InvalidAnnotation.class.getName(),
                (candidate1, current1, errors) -> errors.add("Invalid"));

        ConfigAnnotationsValidator validator = new ConfigAnnotationsValidator(validators);

        List<String> errors = new ArrayList<>();
        validator.validate(newNode(candidate), newNode(current), errors);

        assertEquals(List.of("Invalid"), errors);
    }

    @Retention(RetentionPolicy.RUNTIME)
    @interface InvalidAnnotation {
    }

    private static ConfigNode newNode(ConfigAnnotation... annotations) {
        return new ConfigNode(null, Map.of(), Arrays.asList(annotations), EnumSet.noneOf(Flags.class));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static void expectErrors(Collection<String> expected, List<String> actual) {
        Matcher[] matchers = expected.stream().map(CoreMatchers::containsString).toArray(Matcher[]::new);
        assertThat(actual, CoreMatchers.hasItems(matchers));
    }

    private static <A extends Annotation> ConfigAnnotation getAnnotation(Class<?> clazz,
            String field,
            String annotationName,
            Class<A> annotationClass
    ) {
        try {
            Field f = clazz.getField(field);
            A annotation = f.getAnnotation(annotationClass);
            if (annotation == null) {
                throw new IllegalStateException(format("No annotation: {} found. Class: {}, field: {}", annotationClass, clazz, field));
            }
            return ConfigurationTreeScanner.extractAnnotation(annotationName, annotation);
        } catch (NoSuchFieldException e) {
            throw new IllegalStateException(e);
        }
    }
}
