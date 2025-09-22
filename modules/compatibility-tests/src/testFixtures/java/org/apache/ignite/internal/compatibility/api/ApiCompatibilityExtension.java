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

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.compatibility.api.MethodProvider.looksLikeMethodName;
import static org.junit.platform.commons.support.AnnotationSupport.findAnnotation;
import static org.junit.platform.commons.support.AnnotationSupport.isAnnotated;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.apache.ignite.internal.IgniteVersions;
import org.apache.ignite.internal.IgniteVersions.Version;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;

class ApiCompatibilityExtension implements TestTemplateInvocationContextProvider {
    private static final String[] MODULES_ALL;

    @Override
    public boolean supportsTestTemplate(ExtensionContext context) {
        return isAnnotated(context.getTestMethod(), ApiCompatibilityTest.class);
    }

    @Override
    public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(ExtensionContext context) {
        ApiCompatibilityTest a = findAnnotation(context.getRequiredTestMethod(), ApiCompatibilityTest.class).orElseThrow();

        List<String> oldVersions;
        if (a.oldVersions().length == 0) {
            oldVersions = IgniteVersions.INSTANCE.versions().stream().map(Version::version).distinct().collect(toList());
        } else if (a.oldVersions().length == 1 && looksLikeMethodName(a.oldVersions()[0])) {
            oldVersions = MethodProvider.provideArguments(context, a.oldVersions()[0]);
        } else {
            oldVersions = Arrays.asList(a.oldVersions());
        }

        // to reduce test time - need to resolve dependencies paths here once for all modules
        String[] modules = a.modules().length == 0 ? MODULES_ALL : a.modules();

        return Arrays.stream(modules)
                .flatMap(module -> oldVersions.stream().map(v -> new CompatibilityInput(module, v, a)))
                .map(input -> new ApiCompatibilityTestInvocationContext(input, new TestNameFormatter(context, input)));
    }

    static {
        MODULES_ALL = new String[] { // could be resolved by gradle or moved to public annotation after stabilization
                "ignite-api"
        };
    }
}
