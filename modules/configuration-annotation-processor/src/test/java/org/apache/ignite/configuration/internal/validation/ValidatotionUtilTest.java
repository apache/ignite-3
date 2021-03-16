/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.configuration.internal.validation;

import java.lang.annotation.Annotation;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.configuration.ConfigurationChanger;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.sample.storage.TestConfigurationStorage;
import org.apache.ignite.configuration.tree.InnerNode;
import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.configuration.validation.Validator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** */
public class ValidatotionUtilTest {
    /** */
    @Target(FIELD)
    @Retention(RUNTIME)
    @interface LeafValidation {
    }

    /** */
    @Target(FIELD)
    @Retention(RUNTIME)
    @interface InnerValidation {
    }

    /** */
    @Target(FIELD)
    @Retention(RUNTIME)
    @interface NamedListValidation {
    }

    /** */
    @ConfigurationRoot(rootName = "root", storage = TestConfigurationStorage.class)
    public static class ValidatedRootConfigurationSchema {
        /** */
        @InnerValidation
        @ConfigValue
        public ValidatedChildConfigurationSchema child;

        /** */
        @NamedListValidation
        @NamedConfigValue
        public ValidatedChildConfigurationSchema elements;
    }

    /** */
    @Config
    public static class ValidatedChildConfigurationSchema {
        /** */
        @LeafValidation
        @Value(hasDefault = true)
        public String str = "foo";
    }

    /** */
    private ConfigurationChanger changer;

    /** */
    @BeforeEach
    public void before() {
        changer = new ConfigurationChanger(ValidatedRootConfiguration.KEY);

        changer.register(new TestConfigurationStorage());

        changer.initialize(TestConfigurationStorage.class);
    }

    /** */
    @AfterEach
    public void after() {
        changer = null;
    }

    /** */
    @Test
    void leafNode() throws Exception {
        var root = (ValidatedRootNode)changer.getRootNode(ValidatedRootConfiguration.KEY);

        Map<RootKey<?, ?>, InnerNode> rootsMap = Map.of(ValidatedRootConfiguration.KEY, root);

        Validator<LeafValidation, String> validator = new Validator<>() {
            @Override public void validate(LeafValidation annotation, ValidationContext<String> ctx) {
                ctx.addIssue(new ValidationIssue("foo"));
            }
        };

        Map<Class<? extends Annotation>, Set<Validator<?, ?>>> validators = Map.of(LeafValidation.class, Set.of(validator));

        List<ValidationIssue> issues = ValidationUtil.validate(rootsMap, rootsMap, null, new HashMap<>(), validators);

        assertEquals(1, issues.size());

        assertEquals("foo", issues.get(0).message());
    }
}
