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

package org.apache.ignite.configuration.validation;

import java.lang.annotation.Annotation;
import org.apache.ignite.configuration.RootKey;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Validation context for the validators.
 *
 * @see Validator#validate(Annotation, ValidationContext)
 */
public interface ValidationContext<VIEW> {
    /** @return String representation of currently validated value, i.e. {@code root.config.node} */
    String currentKey();

    /** @return Previous value of the configuration. Might be null for leaves only. */
    @Nullable VIEW getOldValue();

    /** @return Updated value of the configuration. Cannot be null. */
    @NotNull VIEW getNewValue();

    /**
     * @param rootKey Root key.
     * @return Configuration root view before updates. Guaranteed to return valid value only if root belongs to the same
     *      storage as currently validated value. Otherwise result of the method may very between invocations or even
     *      be {@code null} if corresponding storage is not initialized.
     *
     * @param <ROOT> Root view type derived from the root key.
     */
    @Nullable <ROOT> ROOT getOldRoot(RootKey<?, ROOT> rootKey);

    /**
     * @param rootKey Root key.
     * @return Configuration root view after updates. Guaranteed to return valid value only if root belongs to the same
     *      storage as currently validated value. Otherwise result of the method may very between invocations or even
     *      be {@code null} if corresponding storage is not initialized.
     *
     * @param <ROOT> Root view type derived from the root key.
     */
    @Nullable <ROOT> ROOT getNewRoot(RootKey<?, ROOT> rootKey);

    /**
     * Signifies that there's something wrong. Values will be accumulated and passed to the user later.
     *
     * @param issue Validation issue object.
     * @see ConfigurationValidationException
     */
    void addIssue(ValidationIssue issue);
}
