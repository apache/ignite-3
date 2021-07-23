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

package org.apache.ignite.configuration.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * This annotation denotes configuration schema fields that are dynamically created and mapped by name.
 * Example use-cases for this annotation are Ignite node configuration, cache configuration, because nodes and caches
 * can be added dynamically.
 * Every field annotated with this annotation will produce a {@code NamedListConfiguration} field in generated configuration class.
 *
 * <h1 class="header">Example</h1>
 * <pre><code>
 * {@literal @}Config(value = "local", root = true)
 * public class LocalConfigurationSchema {
 *
 *      {@literal @}NamedConfigValue
 *      private SomeOtherConfiguration someOther;
 *
 * }
 * </code></pre>
 */
@Target(FIELD)
@Retention(RUNTIME)
@Documented
public @interface NamedConfigValue {
    /**
     * Key that can be used in HOCON configuration syntax to declare named list with fixed order.
     * <pre><code>
     * {
     *     root : {
     *         namedList : [
     *             {
     *                 syntheticKey : Element1,
     *                 someValue = Value1
     *             },
     *             {
     *                 syntheticKey : Element2,
     *                 someValue = Value2
     *             }
     *         ]
     *     }
     * }
     * </code></pre>
     *
     * @return Name for the synthetic key.
     */
    String syntheticKeyName() default "name";
}
