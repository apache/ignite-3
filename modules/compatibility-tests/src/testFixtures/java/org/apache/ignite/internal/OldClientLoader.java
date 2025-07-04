package org.apache.ignite.internal;

import com.linkedin.cytodynamics.matcher.GlobMatcher;
import com.linkedin.cytodynamics.nucleus.DelegateRelationshipBuilder;
import com.linkedin.cytodynamics.nucleus.IsolationLevel;
import com.linkedin.cytodynamics.nucleus.LoaderBuilder;
import com.linkedin.cytodynamics.nucleus.OriginRestriction;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import org.gradle.tooling.GradleConnector;
import org.gradle.tooling.ProjectConnection;

public class OldClientLoader {
    public static ClassLoader getClientClassloader(String igniteVersion) throws IOException {
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

            // Add tests to the classpath.
            classpath.add(Path.of("build", "classes", "java", "testFixtures").toUri());
            classpath.add(Path.of("build", "classes", "java", "integrationTest").toUri());

            return LoaderBuilder
                    .anIsolatingLoader()
                    .withClasspath(classpath)
                    .withOriginRestriction(OriginRestriction.allowByDefault())
                    .withParentRelationship(DelegateRelationshipBuilder.builder()
                            .withIsolationLevel(IsolationLevel.FULL)
                            .addWhitelistedClassPredicate(new GlobMatcher("java*"))
                            .addWhitelistedClassPredicate(new GlobMatcher("com*"))
                            .addWhitelistedClassPredicate(new GlobMatcher("jdk*"))
                            .addWhitelistedClassPredicate(new GlobMatcher("org.mock*"))
                            .addWhitelistedClassPredicate(new GlobMatcher("org.junit*"))
                            .addWhitelistedClassPredicate(new GlobMatcher("org.ham*"))
                            .build())
                    .build();
        }
    }
}
