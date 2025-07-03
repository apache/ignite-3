package org.apache.ignite.internal;

import com.linkedin.cytodynamics.matcher.GlobMatcher;
import com.linkedin.cytodynamics.nucleus.DelegateRelationshipBuilder;
import com.linkedin.cytodynamics.nucleus.IsolationLevel;
import com.linkedin.cytodynamics.nucleus.LoaderBuilder;
import com.linkedin.cytodynamics.nucleus.OriginRestriction;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.client.IgniteClient;
import org.gradle.tooling.GradleConnector;
import org.gradle.tooling.ProjectConnection;

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
                            .build())
                    .build();

            Class<?> clientClass = loader.loadClass(IgniteClient.class.getName());
            var clientBuilder = clientClass.getDeclaredMethod("builder").invoke(null);

            // TODO: We can build the client, but we can't access it as IgniteClient due to the classloader isolation.
            // 1. Whitelist the API interfaces.
            // 2. Somehow run tests within the isolated classloader.
            System.out.println(clientBuilder);

            clientBuilder.getClass().getDeclaredMethod("addresses", String[].class)
                    .invoke(clientBuilder, (Object) new String[]{"localhost:10800"});

            clientBuilder.getClass().getDeclaredMethod("connectTimeout", long.class)
                    .invoke(clientBuilder, 3000L);

            Object client = clientBuilder.getClass().getDeclaredMethod("build").invoke(clientBuilder);
        } catch (IOException | InvocationTargetException | IllegalAccessException | NoSuchMethodException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
