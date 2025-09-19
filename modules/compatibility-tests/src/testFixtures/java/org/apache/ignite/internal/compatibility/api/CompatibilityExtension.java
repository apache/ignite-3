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

import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

class CompatibilityExtension implements BeforeEachCallback, ParameterResolver {
    private static final Namespace NAMESPACE = Namespace.create(CompatibilityExtension.class);
    private static final String OUTPUT_KEY = "compatibilityOutput";
    private final CompatibilityInput input;

    CompatibilityExtension(CompatibilityInput input) {
        this.input = input;
    }

    @Override
    public void beforeEach(ExtensionContext context) {
        CompatibilityOutput c = CompatibilityChecker.check(input);
        context.getStore(NAMESPACE).put(OUTPUT_KEY, c);
        CompatibilityChecker.generateOutput(c);
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext context)
            throws ParameterResolutionException {
        Class<?> type = parameterContext.getParameter().getType();
        return type == CompatibilityOutput.class;
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext context)
            throws ParameterResolutionException {
        return context.getStore(NAMESPACE).get(OUTPUT_KEY, CompatibilityOutput.class);
    }
}
