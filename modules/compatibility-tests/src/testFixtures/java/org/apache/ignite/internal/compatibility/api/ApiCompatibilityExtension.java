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

import static org.junit.platform.commons.support.AnnotationSupport.findAnnotation;
import static org.junit.platform.commons.support.AnnotationSupport.isAnnotated;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
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
        if (a.oldVersion().isBlank()) {
            oldVersions = IgniteVersions.INSTANCE.versions().stream().map(Version::version).distinct().collect(Collectors.toList());
        } else {
            oldVersions = List.of(a.oldVersion());
        }

        // to reduce test time - need to resolve dependencies paths here once for all modules
        String[] modules = a.modules().length == 0 ? MODULES_ALL : a.modules();

        return Arrays.stream(modules)
                .flatMap(module -> oldVersions.stream().map(v -> new CompatibilityInput(module, v, a)))
                .map(input -> new ApiCompatibilityTestInvocationContext(input, new TestNameFormatter(context, input)));
    }

    static {
        MODULES_ALL = new String[] { // could be resolved by gradle or moved to public annotation after stabilization
                "ignite-api",
                "ignite-binary-tuple",
                "ignite-bytecode",
                "ignite-catalog",
                "ignite-catalog-compaction",
                "ignite-catalog-dsl",
                "ignite-cli",
                "ignite-client",
                "ignite-client-common",
                "ignite-client-handler",
                "ignite-cluster-management",
                "ignite-cluster-metrics",
                "ignite-code-deployment",
                "ignite-compute",
                "ignite-configuration",
                "ignite-configuration-annotation-processor",
                "ignite-configuration-api",
                "ignite-configuration-presentation",
                "ignite-configuration-root",
                "ignite-configuration-storage",
                "ignite-configuration-system",
                "ignite-core",
                "ignite-dev-utilities",
                "ignite-distribution-zones",
                "ignite-error-code-annotation-processor",
                "ignite-eventlog",
                "ignite-failure-handler",
                "ignite-file-io",
                "ignite-file-transfer",
                "ignite-index",
                "ignite-jdbc",
                "ignite-low-watermark",
                "ignite-marshaller-common",
                "ignite-metastorage",
                "ignite-metastorage-api",
                "ignite-metastorage-cache",
                "ignite-metrics",
                "ignite-metrics-exporter-otlp",
                "ignite-network",
                "ignite-network-annotation-processor",
                "ignite-network-api",
                "ignite-page-memory",
                "ignite-partition-distribution",
                "ignite-partition-replicator",
                "ignite-placement-driver",
                "ignite-placement-driver-api",
                "ignite-raft",
                "ignite-raft-api",
                "ignite-replicator",
                "ignite-rest",
                "ignite-rest-api",
                "ignite-rocksdb-common",
                "ignite-runner",
                "ignite-schema",
                "ignite-security",
                "ignite-security-api",
                "ignite-sql-engine",
                "ignite-sql-engine-api",
                "ignite-storage-api",
                "ignite-storage-page-memory",
                "ignite-storage-rocksdb",
                "ignite-system-disaster-recovery",
                "ignite-system-view",
                "ignite-system-view-api",
                "ignite-table",
                "ignite-transactions",
                "ignite-vault",
                "ignite-workers",
        };
    }
}
