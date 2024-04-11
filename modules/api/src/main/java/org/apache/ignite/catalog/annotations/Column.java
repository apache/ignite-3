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

package org.apache.ignite.catalog.annotations;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * Describes a column of the table. Type of the column is derived from the type of the annotated field or method.
 */
@Target({METHOD, FIELD})
@Retention(RUNTIME)
public @interface Column {
    /**
     * The name of the column. By default the name of the field or method is used.
     *
     * @return The name of the column.
     */
    String value() default "";

    /**
     * Whether this column is nullable or not. By default the column is nullable.
     *
     * @return {@code true} if column is nullable.
     */
    boolean nullable() default true;

    /**
     *  The length of the string type.
     *
     * @return The length of the string type.
     */
    int length() default -1;

    /**
     * The precision of the numeric type.
     *
     * @return The precision of the numeric type.
     */
    int precision() default -1;

    /**
     * The scale of the numeric type.
     *
     * @return The scale of the numeric type.
     */
    int scale() default -1;

    /**
     * Full SQL definition of the column type without the name, mutually exclusive with other options.
     *
     * @return SQL definition of the column type.
     */
    String columnDefinition() default "";
}
