package org.apache.ignite.example.code.deployment;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import org.apache.ignite.example.util.DeployComputeUnit;

public class AbstractDeploymentUnitExample {

    private static final Path PROJECT_ROOT =
            Paths.get("").toAbsolutePath();

    private static final Path DEFAULT_CLASSES_DIR =
            PROJECT_ROOT.resolve("examples/java/build/classes/java/main");

    private static final Path DEFAULT_JAR_PATH =
            Path.of("build/libs/deploymentunit-example-1.0.0.jar");

    protected static void processDeploymentUnit(String[] arg) throws IOException {
        Map<String, Object> processedArguments = DeployComputeUnit.processArguments(arg);

        boolean runFromIDE = (boolean) processedArguments.get("runFromIDE");

        Path jarPath = DEFAULT_JAR_PATH;
        String jarPathStr = (String) processedArguments.get("jarPath");

        if (jarPathStr != null && !jarPathStr.isBlank()) {
            jarPath = Path.of(jarPathStr);
        }

        if (runFromIDE) {
            DeployComputeUnit.buildJar(DEFAULT_CLASSES_DIR, jarPath);
        }
    }
}
