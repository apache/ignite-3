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

package org.apache.ignite.example.serialization;

import static org.apache.ignite.example.util.DeployComputeUnit.deployUnit;
import static org.apache.ignite.example.util.DeployComputeUnit.deploymentExists;
import static org.apache.ignite.example.util.DeployComputeUnit.undeployUnit;

import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.example.code.deployment.AbstractDeploymentUnitExample;
import org.apache.ignite.example.util.DeployComputeUnit;

/**
 * This example demonstrates the usage of the
 * {@link IgniteCompute#executeAsync(JobTarget, JobDescriptor, Object)} API.
 *
 * <p>Find instructions on how to run the example in the <code>README.md</code>
 * file located in the "examples" directory root.</p>
 *
 * <h2>Execution Modes</h2>
 *
 * <p>There are two modes of execution:</p>
 *
 * <h3>1. Automated : The JAR Deployment for  deployment unit is automated </h3>
 *
 * <h4>1.1 With IDE</h4>
 * <ul>
 *     <li>
 *         <b>Run from an IDE</b><br>
 *         Launch the example directly from the IDE. If the required deployment
 *         unit is not present, the example automatically builds and deploys the
 *         necessary JAR.
 *     </li>
 * </ul>
 *
 * <h4>1.2 Without IDE</h4>
 * <ul>
 *     <li>
 *         <b>Run from the command line</b><br>
 *         Start the example using a Java command where the classpath includes all required
 *         dependencies:<br>
 *         {@code
 *         java -cp "{user.home}\.m2\repository\org\apache\ignite\ignite-core\3.1.0-SNAPSHOT\
 *         ignite-core-3.1.0-SNAPSHOT.jar{other required jars}"
 *         <example-main-class> runFromIDE=false jarPath="{path-to-examples-jar}"}
 *         <br>
 *         In this mode, {@code runFromIDE=false} indicates command-line execution, and
 *         {@code jarPath} must reference the examples JAR used as the deployment unit.
 *     </li>
 * </ul>
 *
 * <h3>2. Manual (with IDE) :  The JAR Deployment for  deployment unit is manual</h3>
 *
 * <p>Before running this example, complete the following steps related to
 * code deployment:</p>
 *
 * <ol>
 *     <li>
 *         Build the <code>ignite-examples-x.y.z.jar</code> file:<br>
 *         {@code ./gradlew :ignite-examples:jar}
 *     </li>
 *     <li>
 *         Deploy the generated JAR as a deployment unit using the CLI:<br>
 *         {@code
 *         cluster unit deploy computeExampleUnit \
 *         --version 1.0.0 \
 *         --path=$IGNITE_HOME/examples/build/libs/ignite-examples-x.y.z.jar}
 *     </li>
 * </ol>
 */


public class SerializationExample extends AbstractDeploymentUnitExample {

    private static final String DEPLOYMENT_UNIT_NATIVE = "nativeSerializationExampleUnit";
    private static final String DEPLOYMENT_UNIT_CUSTOM = "customPojoSerializationExampleUnit";
    private static final String DEPLOYMENT_UNIT_AUTO = "pojoAutoSerializationExampleUnit";
    private static final String DEPLOYMENT_UNIT_TUPLE = "tupleSerializationExampleUnit";
    private static final String VERSION = "1.0.0";
    private static final Path projectRoot = Paths.get("").toAbsolutePath(); // This resolves ignite-examples/
    private static final Path CLASSES_DIR = projectRoot.resolve("examples/java/build/classes/java/main"); // Compiled output
    private static final Path JAR_PATH = Path.of("build/libs/serialization-example-1.0.0.jar"); // Output jar

    public static void main(String[] args) throws Exception {
        try (IgniteClient client = IgniteClient.builder()
                .addresses("127.0.0.1:10800")
                .build()) {

            processDeploymentUnit(args);

            // 1) Check if deployment unit already exists
            if (deploymentExists(DEPLOYMENT_UNIT_NATIVE, VERSION)) {
                System.out.println("Deployment unit already exists. Skip deploy.");
            } else {
                System.out.println("Deployment unit not found. Deploying...");
                deployUnit(DEPLOYMENT_UNIT_NATIVE, VERSION, JAR_PATH);
                System.out.println(" Deployment completed runNativeSerialization.");
            }

            NativeTypeSerializationExample.runNativeSerialization(client);

            // 2) Check if deployment unit already exists
            if (deploymentExists(DEPLOYMENT_UNIT_TUPLE, VERSION)) {
                System.out.println("Deployment unit already exists. Skip deploy.");
            } else {
                System.out.println("Deployment unit not found. Deploying...");
                deployUnit(DEPLOYMENT_UNIT_TUPLE, VERSION, JAR_PATH);
                System.out.println(" Deployment completed runTupleSerialization.");
            }

            TupleSerializationExample.runTupleSerialization(client);

            // 3) Check if deployment unit already exists
            if (deploymentExists(DEPLOYMENT_UNIT_AUTO, VERSION)) {
                System.out.println("Deployment unit already exists. Skip deploy.");
            } else {
                System.out.println("Deployment unit not found. Deploying...");
                deployUnit(DEPLOYMENT_UNIT_AUTO, VERSION, JAR_PATH);
                System.out.println(" Deployment completed runPojoAutoSerialization.");
            }

            PojoAutoSerializationExample.runPojoAutoSerialization(client);

            // 4) Check if deployment unit already exists
            if (deploymentExists(DEPLOYMENT_UNIT_CUSTOM, VERSION)) {
                System.out.println("Deployment unit already exists. Skip deploy.");
            } else {
                System.out.println("Deployment unit not found. Deploying...");
                deployUnit(DEPLOYMENT_UNIT_CUSTOM, VERSION, JAR_PATH);
                System.out.println(" Deployment completed runPojoCustomJsonSerialization.");
            }
            CustomPojoSerializationExample.runPojoCustomJsonSerialization(client);

        } finally {

            System.out.println("Cleaning up resources...");
            undeployUnit(DEPLOYMENT_UNIT_CUSTOM, VERSION);
            undeployUnit(DEPLOYMENT_UNIT_AUTO, VERSION);
            undeployUnit(DEPLOYMENT_UNIT_NATIVE, VERSION);
            undeployUnit(DEPLOYMENT_UNIT_TUPLE, VERSION);

        }
    }
}
