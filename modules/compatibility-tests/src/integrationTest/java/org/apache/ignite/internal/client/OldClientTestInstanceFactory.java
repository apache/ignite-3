package org.apache.ignite.internal.client;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Optional;
import java.util.Set;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.CompatibilityTestBase;
import org.apache.ignite.internal.IgniteCluster;
import org.apache.ignite.internal.OldClientLoader;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestInstanceFactory;
import org.junit.jupiter.api.extension.TestInstanceFactoryContext;
import org.junit.jupiter.api.extension.TestInstantiationException;

public class OldClientTestInstanceFactory implements TestInstanceFactory {
    @Override
    public Object createTestInstance(TestInstanceFactoryContext factoryContext, ExtensionContext extensionContext)
            throws TestInstantiationException {
        try {
            Optional<Object> outerInstance = factoryContext.getOuterInstance();
            Class<?> testClass = factoryContext.getTestClass();
            assert outerInstance.isEmpty() : "Unexpected outer instance for test class: " + testClass.getSimpleName();

            // TODO: Resource management.
            IgniteCluster igniteCluster = startCluster(factoryContext, extensionContext);
            ClientCompatibilityTests delegate = createInstance();

            var testInstance = new OldClientWithCurrentServerCompatibilityTest();
            testInstance.setDelegate(delegate);
            testInstance.createDefaultTables(igniteCluster.createClient());

            return testInstance;
        }
        catch (Exception e) {
            throw new TestInstantiationException(e.getMessage(), e);
        }
    }

    private static IgniteCluster startCluster(TestInstanceFactoryContext factoryContext, ExtensionContext extensionContext) throws IOException {
        TestInfo testInfo = new TestInfo() {
            @Override
            public String getDisplayName() {
                return factoryContext.getTestClass().getSimpleName();
            }

            @Override
            public Set<String> getTags() {
                return Set.of();
            }

            @Override
            public Optional<Class<?>> getTestClass() {
                return Optional.of(factoryContext.getTestClass());
            }

            @Override
            public Optional<Method> getTestMethod() {
                return Optional.empty();
            }
        };

        var workDir = WorkDirectoryExtension.createWorkDir(extensionContext);

        IgniteCluster cluster = CompatibilityTestBase.createCluster(testInfo, workDir);
        cluster.startEmbedded(1, true);

        return cluster;
    }

    private static ClientCompatibilityTests createInstance()
            throws Exception {

        var loader = OldClientLoader.getClientClassloader("3.0.0");

        // Load test class in the old client classloader.
        Class<?> testClass = loader.loadClass(OldClientWithCurrentServerCompatibilityTest.class.getName());
        Object testInstance = testClass.getDeclaredConstructor().newInstance();

        Object clientBuilder = loader.loadClass(IgniteClient.class.getName()).getDeclaredMethod("builder").invoke(null);
        testClass.getMethod("initClient", clientBuilder.getClass()).invoke(testInstance, clientBuilder);

        return proxy(ClientCompatibilityTests.class, testInstance);
    }

    private static <T> T proxy(Class<T> iface, Object obj) {
        return (T) Proxy.newProxyInstance(
                iface.getClassLoader(),
                new Class[]{iface},
                (proxy, method, args) -> obj.getClass().getMethod(method.getName(), method.getParameterTypes()).invoke(obj, args));
    }
}
