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

package org.apache.ignite.internal.configuration.validation;

import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.configuration.validation.Validator;

/**
 * Collection of default configuration validators.
 */
public final class DefaultValidators {

    /**
     * Returns a set of default configuration validators.
     *
     * @return Set of default configuration validators.
     */
    public static Set<Validator<?, ?>> validators() {
        Set<Validator<?, ?>> validators = new HashSet<>();

        validators.add(new ImmutableValidator());
        validators.add(new OneOfValidator());
        validators.add(new ExceptKeysValidator());
        validators.add(new PowerOfTwoValidator());
        validators.add(new RangeValidator());

        return validators;
    }
}
