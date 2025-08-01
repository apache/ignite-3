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
 * This annotation allows defining configuration property name that is otherwise impossible to define in configuration schema. For example,
 * if it matches a keyword of the Java language, or if it results in name conflicts in generated {@code Configuration}/{@code View} classes.
 */
@Target(FIELD)
@Retention(RUNTIME)
@Documented
public @interface PublicName {
    /**
     * Public configuration property name. This name, if present, is used to store this configuration in configuration storage and to render
     * the configuration to the user. Empty string means that public name matches the schema field name.
     */
    String value() default "";

    /**
     * An array of old deprecated names for the configuration. Any of these names should be accounted for when parsing configuration from
     * any source, but avoided when showing to the user or saving to the configuration storage. These names should also be deleted from the
     * corresponding configuration storage upon encountering.
     */
    String[] legacyNames() default {};
}
