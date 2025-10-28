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

package org.apache.ignite.internal.client;

import static org.apache.ignite.internal.CompatibilityTestBase.baseVersions;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.Cluster.ServerRegistration;
import org.apache.ignite.internal.CompatibilityTestBase;
import org.apache.ignite.internal.IgniteCluster;
import org.apache.ignite.internal.OldClientLoader;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.network.ClusterNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.AfterParameterizedClassInvocation;
import org.junit.jupiter.params.BeforeParameterizedClassInvocation;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests that old Java client can work with the current server version.
 */
@SuppressWarnings("ThrowableNotThrown")
@ExtendWith(WorkDirectoryExtension.class)
@TestInstance(Lifecycle.PER_CLASS)
@ParameterizedClass
@MethodSource("clientVersions")
public class OldClientWithCurrentServerCompatibilityTest extends BaseIgniteAbstractTest implements ClientCompatibilityTests {
    @Parameter
    private String clientVersion;

    private IgniteCluster cluster;

    private ClientCompatibilityTests delegate;

    @BeforeParameterizedClassInvocation
    void beforeAll(String clientVer, TestInfo testInfo, @WorkDirectory Path workDir) throws Exception {
        clientVersion = clientVer;

        cluster = CompatibilityTestBase.createCluster(testInfo, workDir, CompatibilityTestBase.NODE_BOOTSTRAP_CFG_TEMPLATE);
        List<ServerRegistration> serverRegistrations = cluster.startEmbeddedNotInitialized(1);
        cluster.initEmbedded(serverRegistrations, x -> {});

        initTestData(cluster.node(0));

        delegate = createTestInstanceWithOldClient(clientVersion);
    }

    @AfterParameterizedClassInvocation
    void afterAll() throws Exception {
        IgniteUtils.closeAllManually(
                () -> delegate.close(),
                () -> cluster.stop());
    }

    @Override
    public IgniteClient client() {
        throw new UnsupportedOperationException("Do not use. When adding new tests, override via delegate field.");
    }

    @Override
    public AtomicInteger idGen() {
        throw new UnsupportedOperationException("Do not use. When adding new tests, override via delegate field.");
    }

    @Test
    @Override
    public void testClusterNodes() {
        if ("3.0.0".equals(clientVersion)) {
            // 3.0.0 client does not have cluster.nodes() method.
            assertThrowsWithCause(() -> delegate.testClusterNodes(), NoSuchMethodError.class);
            return;
        }

        delegate.testClusterNodes();
    }

    @Test
    @Override
    public void testClusterNodesDeprecated() {
        delegate.testClusterNodesDeprecated();
    }

    @Test
    @Override
    public void testTableByName() {
        delegate.testTableByName();
    }

    @Test
    @Override
    public void testTableByQualifiedName() {
        if ("3.0.0".equals(clientVersion)) {
            // 3.0.0 client does not have qualified names.
            assertThrowsWithCause(() -> delegate.testTableByQualifiedName(), NoSuchMethodError.class);
            return;
        }

        delegate.testTableByQualifiedName();
    }

    @Test
    @Override
    public void testTables() {
        delegate.testTables();
    }

    @Test
    @Override
    public void testSqlColumnMeta() {
        delegate.testSqlColumnMeta();
    }

    @Test
    @Override
    public void testSqlSelectAllColumnTypes() {
        delegate.testSqlSelectAllColumnTypes();
    }

    @Test
    @Override
    public void testSqlMultiplePages() {
        delegate.testSqlMultiplePages();
    }

    @Test
    @Override
    public void testSqlScript() {
        delegate.testSqlScript();
    }

    @Test
    @Override
    public void testSqlBatch() {
        delegate.testSqlBatch();
    }

    @Test
    @Override
    public void testRecordViewOperations() {
        delegate.testRecordViewOperations();
    }

    @Test
    @Override
    public void testKvViewOperations() {
        delegate.testKvViewOperations();
    }

    @Test
    @Override
    public void testRecordViewAllColumnTypes() {
        delegate.testRecordViewAllColumnTypes();
    }

    @Test
    @Override
    public void testTxCommit() {
        delegate.testTxCommit();
    }

    @Test
    @Override
    public void testTxRollback() {
        delegate.testTxRollback();
    }

    @Test
    @Override
    public void testTxReadOnly() {
        delegate.testTxReadOnly();
    }

    @Test
    @Override
    public void testComputeMissingJob() {
        delegate.testComputeMissingJob();
    }

    @Override
    @ParameterizedTest
    @MethodSource("jobArgs")
    public void testComputeArgs(Object arg) {
        delegate.testComputeArgs(arg);
    }

    @Test
    @Override
    public void testComputeExecute() {
        delegate.testComputeExecute();
    }

    @Test
    @Override
    public void testComputeExecuteColocated() {
        delegate.testComputeExecuteColocated();
    }

    @Test
    @Override
    public void testComputeExecuteBroadcast() {
        delegate.testComputeExecuteBroadcast();
    }

    @Test
    @Override
    public void testComputeExecuteBroadcastTable() {
        delegate.testComputeExecuteBroadcastTable();
    }

    @Test
    @Override
    public void testStreamer() {
        delegate.testStreamer();
    }

    @Override
    public void testStreamerWithReceiver() {
        delegate.testStreamerWithReceiver();
    }

    @Override
    public void testStreamerWithReceiverArg() {
        delegate.testStreamerWithReceiverArg();
    }

    private static ClientCompatibilityTests createTestInstanceWithOldClient(String igniteVersion)
            throws Exception {
        var loader = OldClientLoader.getIsolatedClassLoader(igniteVersion);

        // Load test class instance in the old client classloader.
        Object clientBuilder = loader.loadClass(IgniteClient.class.getName()).getDeclaredMethod("builder").invoke(null);
        Constructor<?> testCtor = loader.loadClass(Delegate.class.getName()).getDeclaredConstructor(clientBuilder.getClass());
        testCtor.setAccessible(true);
        Object testInstance = testCtor.newInstance(clientBuilder);

        // Wrap the test instance from another classloader using the interface from the current classloader.
        return proxy(ClientCompatibilityTests.class, testInstance);
    }

    private static <T> T proxy(Class<T> iface, Object obj) {
        return (T) Proxy.newProxyInstance(
                iface.getClassLoader(),
                new Class[]{iface},
                (proxy, method, args) -> {
                    Method targetMethod = obj.getClass().getMethod(method.getName(), method.getParameterTypes());
                    targetMethod.setAccessible(true);
                    return targetMethod.invoke(obj, args);
                });
    }

    private static List<String> clientVersions() {
        return baseVersions();
    }

    private static class Delegate implements ClientCompatibilityTests {
        private final AtomicInteger idGen = new AtomicInteger(1000);

        private final IgniteClient client;

        private Delegate(IgniteClient.Builder client) {
            this.client = client.addresses("localhost:10800").build();
        }

        @Override
        public IgniteClient client() {
            return client;
        }

        @Override
        public AtomicInteger idGen() {
            return idGen;
        }

        @Override
        public void close() {
            client.close();
        }

        @SuppressWarnings("deprecation") // Old client uses deprecated method.
        @Override
        public Collection<ClusterNode> clusterNodes() {
            return client.clusterNodes();
        }
    }
}
