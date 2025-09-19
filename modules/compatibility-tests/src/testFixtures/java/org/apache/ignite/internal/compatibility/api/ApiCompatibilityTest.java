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

package org.apache.ignite.internal.compatibility.api;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * {@code @ApiCompatibilityTest} is used to signal that the annotated method is a API compatibility test between two versions of a module.
 *
 * <p>This annotation is somewhat similar to {@code @ParameterizedTest}, as in it also can run test multiple times per module.
 *
 * <p>Methods annotated with this annotation should not be annotated with {@code Test}.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@TestTemplate
@ExtendWith(ApiCompatibilityExtension.class)
public @interface ApiCompatibilityTest {

    /**
     * Module old versions to check compatibility against.
     *
     * <p>If empty, uses {@code versions.json}.
     *
     * <p>If single value is given and it looks like a static factory method, this method is used to provide versions.
     * Factory methods in external classes must be referenced by
     * <em>fully qualified method name</em> &mdash; for example,
     * {@code "com.example.StringsProviders#blankStrings"} or
     * {@code "com.example.TopLevelClass$NestedClass#classMethod"} for a factory
     * method in a static nested class.
     */
    String[] oldVersions() default {};

    /**
     * Module new version to check compatibility for. If empty, current version is used.
     */
    String newVersion() default "";

    /**
     * List of modules to check. If empty, all modules are checked.
     */
    String[] modules() default { "ignite-api" };

    /**
     * Semicolon separated list of elements to exclude in the form
     * {@code package.Class#classMember}, * can be used as wildcard. Annotations
     * are given as FQN starting with @.
     * <br>Examples:<br>
     * {@code mypackage;my.Class;other.Class#method(int,long);foo.Class#field;@my.Annotation}.
     */
    String exclude() default "";

    /**
     * Exit with an error if any incompatibility is detected.
     */
    boolean errorOnIncompatibility() default true;
}
