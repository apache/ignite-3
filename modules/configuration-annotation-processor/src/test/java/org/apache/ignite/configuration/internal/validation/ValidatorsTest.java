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

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import org.apache.ignite.configuration.ConfigurationChanger;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.sample.storage.TestConfigurationStorage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/** */
public class ValidatorsTest {
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
    }
}
