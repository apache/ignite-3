package org.apache.ignite.internal;

import com.linkedin.cytodynamics.nucleus.DelegateRelationshipBuilder;
import com.linkedin.cytodynamics.nucleus.IsolationLevel;
import com.linkedin.cytodynamics.nucleus.LoaderBuilder;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import org.gradle.tooling.GradleConnector;
import org.gradle.tooling.ProjectConnection;
import org.gradle.tooling.model.build.BuildEnvironment;

public class ClientRunner {
    public static void runClient(String igniteVersion) {
        // 1. Use constructArgFile to resolve dependencies of a given client version.
        // 2. Use Cytodynamics to run the client with the constructed arg file in an isolated classloader.
        try (ProjectConnection connection = GradleConnector.newConnector()
                // Current directory is modules/compatibility-tests so get two parents
                .forProjectDirectory(Path.of("..", "..").normalize().toFile())
                .connect()
        ) {
            BuildEnvironment environment = connection.model(BuildEnvironment.class).get();

            File javaHome = environment.getJava().getJavaHome();

            // TODO: Move constructArgFile to a utility class or method.
            File argFile = IgniteCluster.constructArgFile(connection, "org.apache.ignite:ignite-client:" + igniteVersion);

            System.out.println(argFile.getAbsolutePath());

            ClassLoader loader = LoaderBuilder
                    .anIsolatingLoader()
                    .withClasspath(List.of(argFile.toURI()))
                    .withParentRelationship(DelegateRelationshipBuilder.builder()
                            .withIsolationLevel(IsolationLevel.FULL)
                            .build())
                    .build();

//            MyApi myApiImpl = (MyApi) loader.loadClass("com.myapi.MyApiImpl").newInstance();
//
//            myApiImpl.doIt();
//
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
