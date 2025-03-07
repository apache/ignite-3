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

package org.apache.ignite.internal.catalog.storage.serialization;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * This annotation is used to dynamically create a registry of all catalog serializers.
 *
 * <p>All {@link CatalogObjectSerializer catalog object serializers} must be marked with this annotation.
 *
 * @see CatalogObjectSerializer
 */
@Target(ElementType.TYPE)
@Retention(RUNTIME)
public @interface CatalogSerializer {
    /**
     * Returns serializer version.
     *
     * <p>Versions start at 1 and increase by one.
     */
    short version();

    /**
     * The product version starting from which the serializer is used.
     *
     * <p>For example - {@code 3.0.0}.
     */
    String since();
}
