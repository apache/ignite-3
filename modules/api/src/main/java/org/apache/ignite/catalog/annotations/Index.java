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

import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import org.apache.ignite.catalog.IndexType;

/**
 * Describes an index.
 */
@Target({})
@Retention(RUNTIME)
public @interface Index {
    /**
     * The name of the index. If it's empty, the name will be autogenerated from the column names.
     *
     * @return The name of the index.
     */
    String value() default "";

    /**
     * Columns to include in the index.
     *
     * @return Columns to include in the index.
     */
    Col[] columns();

    /**
     * The type of the index.
     *
     * @return The type of the index.
     */
    IndexType type() default IndexType.DEFAULT;
}
