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

package org.apache.ignite.internal.configuration.validation;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.configuration.validation.ConfigurationValidatorImplTest.PolyValidatedChildConfigurationSchema.DEFAULT_POLY_TYPE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.ConfigurationExtension;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.PolymorphicConfig;
import org.apache.ignite.configuration.annotation.PolymorphicConfigInstance;
import org.apache.ignite.configuration.annotation.PolymorphicId;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.configuration.validation.Validator;
import org.apache.ignite.internal.configuration.ConfigurationTreeGenerator;
import org.apache.ignite.internal.configuration.SuperRoot;
import org.apache.ignite.internal.configuration.asm.ConfigurationAsmGenerator;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.apache.ignite.internal.configuration.util.ConfigurationUtil;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Test class for {@link ConfigurationValidatorImpl}.
 */
@ExtendWith(MockitoExtension.class)
public class ConfigurationValidatorImplTest extends BaseIgniteAbstractTest {

    private static ConfigurationAsmGenerator cgen;

    @Mock
    ConfigurationTreeGenerator configurationTreeGenerator;

    @BeforeAll
    public static void beforeAll() {
        cgen = new ConfigurationAsmGenerator();

        cgen.compileRootSchema(
                ValidatedRootConfigurationSchema.class,
                Map.of(ValidatedChildConfigurationSchema.class, Set.of(InternalValidatedChildConfigurationSchema.class)),
                Map.of(PolyValidatedChildConfigurationSchema.class, Set.of(FirstPolyValidatedChildConfigurationSchema.class))
        );
    }

    @AfterAll
    public static void afterAll() {
        cgen = null;
    }

    @Target(FIELD)
    @Retention(RUNTIME)
    @interface LeafValidation {
    }

    @Target(FIELD)
    @Retention(RUNTIME)
    @interface InnerValidation {
    }

    @Target(FIELD)
    @Retention(RUNTIME)
    @interface NamedListValidation {
    }

    /**
     * Root configuration schema.
     */
    @ConfigurationRoot(rootName = "root", type = ConfigurationType.LOCAL)
    public static class ValidatedRootConfigurationSchema {
        @InnerValidation
        @ConfigValue
        public ValidatedChildConfigurationSchema child;

        @NamedListValidation
        @NamedConfigValue
        public ValidatedChildConfigurationSchema elements;

        @InnerValidation
        @ConfigValue
        public PolyValidatedChildConfigurationSchema poly;

        @NamedListValidation
        @NamedConfigValue
        public PolyValidatedChildConfigurationSchema polyChildren;
    }

    /**
     * Child configuration schema.
     */
    @Config
    public static class ValidatedChildConfigurationSchema {
        @LeafValidation
        @Value(hasDefault = true)
        public String str = "foo";
    }

    /**
     * Child internal extension configuration schema.
     */
    @ConfigurationExtension(internal = true)
    public static class InternalValidatedChildConfigurationSchema extends ValidatedChildConfigurationSchema {
        @LeafValidation
        @Value(hasDefault = true)
        public String strInternal = "fooInternal";
    }

    /**
     * Child polymorphic configuration schema.
     */
    @PolymorphicConfig
    public static class PolyValidatedChildConfigurationSchema {
        public static final String DEFAULT_POLY_TYPE = "first";

        @PolymorphicId(hasDefault = true)
        @LeafValidation
        public String type = DEFAULT_POLY_TYPE;
    }

    /**
     * Child first polymorphic instance configuration schema.
     */
    @PolymorphicConfigInstance(DEFAULT_POLY_TYPE)
    public static class FirstPolyValidatedChildConfigurationSchema extends PolyValidatedChildConfigurationSchema {
        @LeafValidation
        @Value(hasDefault = true)
        public String strPoly = "fooPolyFirst";
    }

    private InnerNode root;

    @BeforeEach
    public void before() {
        root = cgen.instantiateNode(ValidatedRootConfigurationSchema.class);

        ConfigurationUtil.addDefaults(root);
    }

    @Test
    public void validateLeafNode() {
        var rootsNode = new SuperRoot(key -> null, Map.of(ValidatedRootConfiguration.KEY, root));

        Validator<LeafValidation, String> validator = new Validator<>() {
            /** {@inheritDoc} */
            @Override
            public void validate(LeafValidation annotation, ValidationContext<String> ctx) {
                ctx.addIssue(new ExValidationIssue("bar", ctx.currentKey(), ctx.getOldValue(), ctx.getNewValue()));
            }
        };

        Set<Validator<LeafValidation, String>> validators = Set.of(validator);

        ConfigurationValidatorImpl configurationValidator = new ConfigurationValidatorImpl(configurationTreeGenerator, validators);
        List<ValidationIssue> actual = configurationValidator.validate(rootsNode, rootsNode);

        List<ValidationIssue> expected = List.of(
                new ExValidationIssue("bar", "root.child.str", "foo", "foo"),
                new ExValidationIssue("bar", "root.child.strInternal", "fooInternal", "fooInternal"),
                new ExValidationIssue("bar", "root.poly.type", DEFAULT_POLY_TYPE, DEFAULT_POLY_TYPE),
                new ExValidationIssue("bar", "root.poly.strPoly", "fooPolyFirst", "fooPolyFirst")
        );

        assertThat(
                ExValidationIssue.sortedByCurrentKey(actual),
                equalTo(ExValidationIssue.sortedByCurrentKey(expected))
        );
    }

    @Test
    public void validateInnerNode() throws Exception {
        var rootsNode = new SuperRoot(key -> null, Map.of(ValidatedRootConfiguration.KEY, root));

        Validator<InnerValidation, Object> validator = new Validator<>() {
            /** {@inheritDoc} */
            @Override
            public void validate(InnerValidation annotation, ValidationContext<Object> ctx) {
                Object oldValue = ctx.getOldValue();
                Object newValue = ctx.getNewValue();

                if (oldValue instanceof ValidatedChildView) {
                    oldValue = ((ValidatedChildView) oldValue).str();
                    newValue = ((ValidatedChildView) newValue).str();
                } else {
                    oldValue = ((PolyValidatedChildView) oldValue).type();
                    newValue = ((PolyValidatedChildView) newValue).type();
                }

                ctx.addIssue(new ExValidationIssue("bar", ctx.currentKey(), oldValue, newValue));
            }
        };

        Set<Validator<InnerValidation, ?>> validators = Set.of(validator);

        ConfigurationValidatorImpl configurationValidator = new ConfigurationValidatorImpl(configurationTreeGenerator, validators);
        List<ValidationIssue> actual = configurationValidator.validate(rootsNode, rootsNode);

        List<ValidationIssue> expected = List.of(
                new ExValidationIssue("bar", "root.child", "foo", "foo"),
                new ExValidationIssue("bar", "root.poly", DEFAULT_POLY_TYPE, DEFAULT_POLY_TYPE)
        );

        assertThat(
                ExValidationIssue.sortedByCurrentKey(actual),
                equalTo(ExValidationIssue.sortedByCurrentKey(expected))
        );
    }

    @Test
    public void validateNamedListNode() {
        var rootsNode = new SuperRoot(key -> null, Map.of(ValidatedRootConfiguration.KEY, root));

        Validator<NamedListValidation, NamedListView<?>> validator = new Validator<>() {
            /** {@inheritDoc} */
            @Override
            public void validate(NamedListValidation annotation, ValidationContext<NamedListView<?>> ctx) {
                ctx.addIssue(new ExValidationIssue(
                        "bar",
                        ctx.currentKey(),
                        ctx.getOldValue().namedListKeys(),
                        ctx.getNewValue().namedListKeys()
                ));
            }
        };

        Set<Validator<NamedListValidation, NamedListView<?>>> validators = Set.of(validator);

        ConfigurationValidatorImpl configurationValidator = new ConfigurationValidatorImpl(configurationTreeGenerator, validators);
        List<ValidationIssue> actual = configurationValidator.validate(rootsNode, rootsNode);

        List<ValidationIssue> expected = List.of(
                new ExValidationIssue("bar", "root.elements", List.of(), List.of()),
                new ExValidationIssue("bar", "root.polyChildren", List.of(), List.of())
        );

        assertThat(
                ExValidationIssue.sortedByCurrentKey(actual),
                equalTo(ExValidationIssue.sortedByCurrentKey(expected))
        );
    }

    @Test
    void testGetOwner() {
        var rootsNode = new SuperRoot(key -> null, Map.of(ValidatedRootConfiguration.KEY, root));

        Validator<InnerValidation, Object> innerValidator = new Validator<>() {
            /** {@inheritDoc} */
            @Override
            public void validate(InnerValidation annotation, ValidationContext<Object> ctx) {
                Object oldOwner = ctx.getOldOwner();
                Object newOwner = ctx.getOldOwner();

                // Checks that the nested configuration owner is ValidatedRootView.
                if (!(oldOwner instanceof ValidatedRootView)) {
                    ctx.addIssue(new ExValidationIssue("Wrong inner owner", ctx.currentKey(), null, oldOwner));
                }

                // Checks that the nested configuration owner is ValidatedRootView.
                if (!(newOwner instanceof ValidatedRootView)) {
                    ctx.addIssue(new ExValidationIssue("Wrong inner owner", ctx.currentKey(), null, newOwner));
                }
            }
        };

        Validator<LeafValidation, Object> leafValidator = new Validator<>() {
            /** {@inheritDoc} */
            @Override
            public void validate(LeafValidation annotation, ValidationContext<Object> ctx) {
                Object oldOwner = ctx.getOldOwner();
                Object newOwner = ctx.getOldOwner();

                // Checks that the owner of the primitive (leaf) configuration is either ValidatedChildView or PolyValidatedChildView.
                if (!(oldOwner instanceof ValidatedChildView) && !(oldOwner instanceof PolyValidatedChildView)) {
                    ctx.addIssue(new ExValidationIssue("Wrong leaf owner", ctx.currentKey(), null, oldOwner));
                }

                // Checks that the owner of the primitive (leaf) configuration is either ValidatedChildView or PolyValidatedChildView.
                if (!(newOwner instanceof ValidatedChildView) && !(newOwner instanceof PolyValidatedChildView)) {
                    ctx.addIssue(new ExValidationIssue("Wrong leaf owner", ctx.currentKey(), null, newOwner));
                }
            }
        };

        Validator<NamedListValidation, NamedListView<?>> namedListValidator = new Validator<>() {
            /** {@inheritDoc} */
            @Override
            public void validate(NamedListValidation annotation, ValidationContext<NamedListView<?>> ctx) {
                Object oldOwner = ctx.getOldOwner();
                Object newOwner = ctx.getOldOwner();

                // Checks that the named list configuration owner is ValidatedRootView.
                if (!(oldOwner instanceof ValidatedRootView)) {
                    ctx.addIssue(new ExValidationIssue("Wrong list owner", ctx.currentKey(), null, oldOwner));
                }

                // Checks that the named list configuration owner is ValidatedRootView.
                if (!(newOwner instanceof ValidatedRootView)) {
                    ctx.addIssue(new ExValidationIssue("Wrong list owner", ctx.currentKey(), null, newOwner));
                }
            }
        };

        Set<Validator<?, ?>> validators = Set.of(innerValidator, leafValidator, namedListValidator);

        ConfigurationValidatorImpl configurationValidator = new ConfigurationValidatorImpl(configurationTreeGenerator, validators);
        List<ValidationIssue> actual = configurationValidator.validate(rootsNode, rootsNode);

        // Checks that for a nested/leaf/named configuration their owners will be correctly returned.
        assertThat(actual, empty());
    }

    private static class ExValidationIssue extends ValidationIssue {

        @IgniteToStringInclude
        @Nullable
        final Object oldVal;

        @IgniteToStringInclude
        final Object newVal;

        ExValidationIssue(
                String message,
                String currentKey,
                @Nullable Object oldVal,
                Object newVal
        ) {
            super(currentKey, message);
            this.oldVal = oldVal;
            this.newVal = newVal;
        }

        /** {@inheritDoc} */
        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            ExValidationIssue that = (ExValidationIssue) o;

            return Objects.equals(key(), that.key())
                    && Objects.equals(oldVal, that.oldVal)
                    && Objects.equals(newVal, that.newVal);
        }

        /** {@inheritDoc} */
        @Override
        public int hashCode() {
            return Objects.hash(key(), oldVal, newVal);
        }

        /** {@inheritDoc} */
        @Override
        public String toString() {
            return S.toString(ExValidationIssue.class, this, message());
        }

        static int compareByCurrentKey(ValidationIssue o1, ValidationIssue o2) {
            ExValidationIssue ex1 = (ExValidationIssue) o1;
            ExValidationIssue ex2 = (ExValidationIssue) o2;

            return ex1.key().compareTo(ex2.key());
        }

        static List<ExValidationIssue> sortedByCurrentKey(List<ValidationIssue> issues) {
            return issues.stream()
                    .sorted(ExValidationIssue::compareByCurrentKey)
                    .map(ExValidationIssue.class::cast)
                    .collect(toList());
        }
    }
}
