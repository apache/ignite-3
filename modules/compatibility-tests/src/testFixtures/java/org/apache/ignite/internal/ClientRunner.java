package org.apache.ignite.internal;

import static org.mockito.Answers.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

import com.linkedin.cytodynamics.matcher.GlobMatcher;
import com.linkedin.cytodynamics.nucleus.DelegateRelationshipBuilder;
import com.linkedin.cytodynamics.nucleus.IsolationLevel;
import com.linkedin.cytodynamics.nucleus.LoaderBuilder;
import com.linkedin.cytodynamics.nucleus.OriginRestriction;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.table.IgniteTables;
import org.apache.ignite.table.Table;
import org.gradle.tooling.GradleConnector;
import org.gradle.tooling.ProjectConnection;
import org.mockito.Mockito;

public class ClientRunner {
    public static void runClient(String igniteVersion) {
        // 1. Use constructArgFile to resolve dependencies of a given client version.
        // 2. Use Cytodynamics to run the client with the constructed arg file in an isolated classloader.
        try (ProjectConnection connection = GradleConnector.newConnector()
                // Current directory is modules/compatibility-tests so get two parents
                .forProjectDirectory(Path.of("..", "..").normalize().toFile())
                .connect()
        ) {
            // TODO: Move constructArgFile to a utility class or method.
            File argFile = IgniteCluster.constructArgFile(connection, "org.apache.ignite:ignite-client:" + igniteVersion, true);

            List<String> classpathLines = Files.readAllLines(argFile.toPath());

            List<URI> classpath = classpathLines.stream()
                    .map(path -> new File(path).toURI())
                    .collect(Collectors.toList());

            ClassLoader loader = LoaderBuilder
                    .anIsolatingLoader()
                    .withClasspath(classpath)
                    .withOriginRestriction(OriginRestriction.allowByDefault())
                    .withParentRelationship(DelegateRelationshipBuilder.builder()
                            .withIsolationLevel(IsolationLevel.FULL)
                            .addWhitelistedClassPredicate(new GlobMatcher("java*"))
                            .addWhitelistedClassPredicate(new GlobMatcher("com*"))
                            .addWhitelistedClassPredicate(new GlobMatcher("jdk*"))
                            .addWhitelistedClassPredicate(new GlobMatcher("org.mock*"))
                            .build())
                    .build();

            System.out.println("Starting client...");
            Class<?> clientClass = loader.loadClass(IgniteClient.class.getName());
            Object clientBuilder = clientClass.getDeclaredMethod("builder").invoke(null);

            // Create test instance and pass the builder.
            // TODO: Load tests in isolated classloader.
            Class<?> testClass = loader.loadClass("org.apache.ignite.internal.client.OldClientWithCurrentServerCompatibilityTest");
            Object testInstance = testClass.getDeclaredConstructor().newInstance();
            testClass.getMethod("initClient", IgniteClient.Builder.class).invoke(testInstance, clientBuilder);

//
//            Class<?> clientBuilderClass = loader.loadClass(IgniteClient.Builder.class.getName());
//            var clientBuilder = clientBuilderClass.getDeclaredConstructor().newInstance();
//
//            System.out.println(clientBuilder);
//
//            clientBuilder.getClass().getDeclaredMethod("addresses", String[].class)
//                    .invoke(clientBuilder, (Object) new String[]{"localhost:10800"});
//
//            clientBuilder.getClass().getDeclaredMethod("connectTimeout", long.class)
//                    .invoke(clientBuilder, 3000L);
//
//            Object clientObj = clientBuilder.getClass().getDeclaredMethod("build").invoke(clientBuilder);
//
//            Ignite ignite = wrapAs(Ignite.class, clientObj);
//
//            ignite.sql().executeScript("CREATE TABLE IF NOT EXISTS test_table (id INT PRIMARY KEY, name VARCHAR)");
//
//            IgniteTables tables = ignite.tables();
//
//            List<Table> tablesList = tables.tables();
//
//            System.out.println("Connected to Ignite server. Available tables:");
//
//            for (Table table : tablesList) {
//                System.out.println(" - " + table.name());
//            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    private static <T> T wrapAs(Class<T> iface, Object obj) {
        if (!iface.isInterface()) {
            return (T) obj;
        }

        return (T) Proxy.newProxyInstance(iface.getClassLoader(), new Class[]{iface}, (proxy, method, args) -> {
            Method delegateMethod = obj.getClass().getMethod(method.getName(), method.getParameterTypes());

            delegateMethod.setAccessible(true);
            Object res = delegateMethod.invoke(obj, args);

            return wrapAs(method.getReturnType(), res);
        });
    }
}
