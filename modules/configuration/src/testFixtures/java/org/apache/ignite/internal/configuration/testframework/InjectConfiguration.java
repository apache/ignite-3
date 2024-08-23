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

package org.apache.ignite.internal.configuration.testframework;

import static org.apache.ignite.configuration.annotation.ConfigurationType.LOCAL;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.apache.ignite.configuration.SuperRootChange;
import org.apache.ignite.configuration.annotation.ConfigurationExtension;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.configuration.annotation.PolymorphicConfig;
import org.apache.ignite.configuration.annotation.PolymorphicConfigInstance;
import org.apache.ignite.internal.configuration.ConfigurationChanger;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.intellij.lang.annotations.Language;

/**
 * Annotation for injecting configuration instances into tests.
 * <p/>
 * This annotation should be used on either fields or method parameters of the {@code *Configuration} type.
 * <p/>
 * Injected instance is initialized with values passed in {@link #value()}, with schema defaults where explicit initial values are not
 * found.
 * <p/>
 * Although configuration instance is mutable, there's no {@link ConfigurationRegistry} and {@link ConfigurationChanger} underneath. Main
 * point of the extension is to provide mocks.
 *
 * @see ConfigurationExtension
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.PARAMETER})
public @interface InjectConfiguration {
    /**
     * Configuration values to initialize the instance. Has HOCON syntax. Must have a root value {@code mock}.
     * <p/>
     * Examples:
     * <ul>
     *     <li>{@code mock.timeout=1000}</li>
     *     <li>{@code mock{cfg1=50, cfg2=90}}</li>
     * </ul>
     * <p/>
     * Uses only default values by default.
     *
     * @return Initial configuration values in HOCON format.
     */
    @Language("HOCON")
    String value() default "mock : {}";

    /**
     * Name value to imitate named list elements. Default empty string value is treated like the absence of the name.
     */
    String name() default "";

    /**
     * Root name of the configuration. Default empty string value is treated like the absence of the root name. The root name is used to
     * patch the configuration tree with dynamic configuration defaults
     * {@link org.apache.ignite.configuration.ConfigurationModule#patchConfigurationWithDynamicDefaults(SuperRootChange)}
     */
    String rootName() default "";

    /**
     * Type of the configuration. Used to select the modules to patch the configuration tree with dynamic configuration defaults.
     */
    ConfigurationType type() default LOCAL;

    /**
     * Array of configuration schema extensions. Every class in the array must be annotated with {@link ConfigurationExtension} and extend
     * some public configuration.
     *
     * @return Array of configuration schema extensions.
     */
    Class<?>[] extensions() default {};

    /**
     * Array of configuration schema extensions. Every class in the array must be annotated with
     * {@link PolymorphicConfigInstance} and extend some {@link PolymorphicConfig} schema.
     *
     * @return Array of configuration schema extensions.
     */
    Class<?>[] polymorphicExtensions() default {};
}
