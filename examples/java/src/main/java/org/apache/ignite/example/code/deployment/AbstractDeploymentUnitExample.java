package org.apache.ignite.example.code.deployment;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import org.apache.ignite.example.util.DeployComputeUnit;

public class AbstractDeploymentUnitExample {


    protected static final Path projectRoot = Paths.get("").toAbsolutePath(); // This resolves ignite-examples/


    // Default Classes Dir when running from IDE/ Commandline
    protected static final Path DEFAULT_CLASSES_DIR = projectRoot.resolve("examples/java/build/classes/java/main"); // Compiled output

    // Default Build Dir when running from IDE/ Commandline
    protected static Path DEFAULT_JAR_PATH = Path.of("build/libs/deploymentunit-example-1.0.0.jar"); // Output jar

    protected static String jarPathAsString = "";
    protected static Path jarPath = DEFAULT_JAR_PATH;
    protected static boolean runFromIDE = true;


    protected static void processDeploymentUnit(String[] arg) throws IOException {
        Map<String, Object> processedArguments = DeployComputeUnit.processArguments(arg);
        runFromIDE = (boolean) processedArguments.get("runFromIDE");
        jarPathAsString = (String) processedArguments.get("jarPath");
        if (jarPathAsString != null && !jarPathAsString.trim().equals("")) {
            jarPath = Path.of(jarPathAsString);
        }
        if (runFromIDE) {
            DeployComputeUnit.buildJar(DEFAULT_CLASSES_DIR, DEFAULT_JAR_PATH);
        }
    }


}