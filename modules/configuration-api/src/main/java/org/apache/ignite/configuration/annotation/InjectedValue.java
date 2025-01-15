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

package org.apache.ignite.configuration.annotation;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * This annotations is intended to be placed on {@link NamedConfigValue} elements to emulate key-value pairs in configuration schemas.
 *
 * <p>Therefore, this annotation induces the following constraints:
 *
 * <ol>
 *     <li>Must be placed on a field of a configuration schema that represents a Named List element;</li>
 *     <li>Must be the only field in the configuration schema, apart from the one marked with {@link InjectedName}.</li>
 * </ol>
 *
 * <p>In all other aspects it behaves exactly like the {@link Value} annotation.
 *
 * <p>For example, this annotation can be used to declare a configuration schema with arbitrary {@code String} properties:
 *
 * <pre><code> {@literal @}Config
 * class PropertyConfigurationSchema {
 *    {@literal @}NamedConfigValue
 *     public PropertyEntryConfigurationSchema properties;
 * }</code></pre>
 * <pre><code> {@literal @}Config
 * class PropertyEntryConfigurationSchema {
 *    {@literal @}InjectedName
 *     public String propertyName;
 *
 *    {@literal @}InjectedValue
 *     public String propertyValue;
 * }</code></pre>
 *
 * <p>This will allow to use the following HOCON to represent this configuration:
 *
 * <pre>{@code
 *     root.properties {
 *         property1: "value1",
 *         property2: "value2",
 *         property3: "value3"
 *     }
 * }</pre>
 */
@Target(FIELD)
@Retention(RUNTIME)
@Documented
public @interface InjectedValue {
    /**
     * Indicates that the current configuration value has a default value. Value itself is derived from the instantiated object of a
     * corresponding schema type. This means that the default is not necessarily a constant value.
     *
     * @return {@code hasDefault} flag value.
     */
    boolean hasDefault() default false;
}
