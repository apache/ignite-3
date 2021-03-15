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

package org.apache.ignite.configuration.sample;

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
import static org.apache.ignite.configuration.sample.storage.AConfiguration.KEY;

/** */
public class ValidatorsTest {
    /** */
    @Target(FIELD)
    @Retention(RUNTIME)
    @interface LeafValiation {
    }

    /** */
    @Target(FIELD)
    @Retention(RUNTIME)
    @interface InnerValiation {
    }

    /** */
    @Target(FIELD)
    @Retention(RUNTIME)
    @interface NamedListValiation {
    }

    /** */
    @ConfigurationRoot(rootName = "root", storage = TestConfigurationStorage.class)
    public static class ValidatedRootConfigurationSchema {
        /** */
        @InnerValiation
        @ConfigValue
        public ValidatedChildConfigurationSchema child;

        /** */
        @NamedListValiation
        @NamedConfigValue
        public ValidatedChildConfigurationSchema elements;
    }

    /** */
    @Config
    public static class ValidatedChildConfigurationSchema {
        /** */
        @LeafValiation
        @Value(hasDefault = true)
        public String str = "foo";
    }

    /** */
    private ConfigurationChanger changer;

    /** */
    @BeforeEach
    public void before() {
        changer = new ConfigurationChanger(KEY);

        changer.init(new TestConfigurationStorage());
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
