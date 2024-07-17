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

package org.apache.ignite.internal.configuration.tree;

import java.lang.reflect.Field;
import org.apache.ignite.configuration.annotation.Secret;

/**
 * Builder for {@link ConverterToMapVisitor}.
 */
public final class ConverterToMapVisitorBuilder {

    /** Include internal configuration nodes (private configuration extensions). */
    private boolean includeInternal = true;

    /** Skip nulls, empty Maps and empty lists. */
    private boolean skipEmptyValues = false;

    /** Mask values, if their {@link Field} has {@link Secret} annotation. */
    private boolean maskSecretValues = false;

    ConverterToMapVisitorBuilder() {
    }

    public ConverterToMapVisitorBuilder includeInternal(boolean includeInternal) {
        this.includeInternal = includeInternal;
        return this;
    }

    public ConverterToMapVisitorBuilder skipEmptyValues(boolean skipEmptyValues) {
        this.skipEmptyValues = skipEmptyValues;
        return this;
    }

    public ConverterToMapVisitorBuilder maskSecretValues(boolean maskSecretValues) {
        this.maskSecretValues = maskSecretValues;
        return this;
    }

    public ConverterToMapVisitor build() {
        return new ConverterToMapVisitor(includeInternal, skipEmptyValues, maskSecretValues);
    }
}
