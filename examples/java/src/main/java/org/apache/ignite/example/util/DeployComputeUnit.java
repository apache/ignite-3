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

package org.apache.ignite.example.util;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

/**
 * Utility class for deploying Ignite compute units.
 *
 * <p>The deployment unit JAR is embedded as a classpath resource at {@code units/deploymentunit-example-1.0.0.jar}
 * by the Gradle {@code processResources} task (which depends on {@code deploymentUnitJar}).
 * This means the JAR is always available on the classpath without requiring any file-system paths
 * or runner-specific workarounds.</p>
 */
public class DeployComputeUnit {

    private static final String DEPLOYMENT_UNIT_JAR_RESOURCE = "units/deploymentunit-example-1.0.0.jar";

    private static final String DEPLOYMENT_UNIT_JAR_FILENAME = "deploymentunit-example-1.0.0.jar";

    private static final String BASE_URL = "http://localhost:10300";

    private static final HttpClient HTTP = HttpClient.newHttpClient();

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final int DEPLOYMENT_TIMEOUT_SECONDS = 30;

    /**
     * Checks if a deployment unit already exists on the cluster with DEPLOYED status.
     *
     * @param unitId Deployment unit ID.
     * @param version Deployment version.
     * @return True if deployment exists with DEPLOYED status.
     * @throws Exception If request fails.
     */
    private static boolean isDeployed(String unitId, String version) throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(new URI(BASE_URL + "/management/v1/deployment/cluster/units/" + unitId))
                .GET()
                .build();

        HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());

        if (resp.statusCode() == 404) {
            // Unit doesn't exist yet
            return false;
        }

        if (resp.statusCode() != 200) {
            throw new RuntimeException("Failed to check deployment status. HTTP " + resp.statusCode() + ": " + resp.body());
        }

        // Parse JSON response - the API returns a Collection<UnitStatus>
        JsonNode root = OBJECT_MAPPER.readTree(resp.body());

        // Handle empty response (unit exists but no matching status)
        if (root.isArray() && root.isEmpty()) {
            return false;
        }

        // The response is an array of UnitStatus objects
        if (!root.isArray()) {
            throw new RuntimeException("Unexpected response format. Expected array, got: " + root);
        }

        // Check if any node has this version deployed
        for (JsonNode unitStatus : root) {
            JsonNode versionToStatus = unitStatus.path("versionToStatus");

            if (versionToStatus.isArray()) {
                for (JsonNode versionStatus : versionToStatus) {
                    String versionValue = versionStatus.path("version").asText();
                    String statusValue = versionStatus.path("status").asText();

                    if (version.equals(versionValue) && "DEPLOYED".equals(statusValue)) {
                        return true;
                    }
                }
            }
        }

        return false;
    }

    /**
     * Checks if a deployment unit exists (in any state).
     *
     * @param unitId Deployment unit ID.
     * @param version Deployment version.
     * @return True if deployment exists.
     * @throws Exception If request fails.
     */
    private static boolean deploymentExists(String unitId, String version) throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(new URI(BASE_URL + "/management/v1/deployment/cluster/units/" + unitId))
                .GET()
                .build();

        HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
        return resp.statusCode() == 200 && resp.body().contains("\"version\":\"" + version + "\"");
    }

    /**
     * Deploys a unit to the Ignite cluster and waits for it to reach DEPLOYED status.
     *
     * <p>The deployment unit JAR is loaded from the classpath resource
     * {@code units/deploymentunit-example-1.0.0.jar}, which is embedded during the Gradle build.</p>
     *
     * @param unitId Deployment unit ID.
     * @param version Deployment version.
     * @throws Exception If deployment fails.
     */
    private static void deployUnit(String unitId, String version) throws Exception {
        byte[] jarBytes;

        try (InputStream jarStream = DeployComputeUnit.class.getClassLoader()
                .getResourceAsStream(DEPLOYMENT_UNIT_JAR_RESOURCE)) {
            if (jarStream == null) {
                throw new IllegalStateException(
                        "Deployment unit JAR not found in classpath at: " + DEPLOYMENT_UNIT_JAR_RESOURCE + "\n"
                                + "Please build the project first: ./gradlew :ignite-examples:build"
                );
            }
            jarBytes = jarStream.readAllBytes();
        }

        String boundary = "igniteBoundary";

        String start =
                "--" + boundary + "\r\n"
                        + "Content-Disposition: form-data; name=\"unitContent\"; filename=\"" + DEPLOYMENT_UNIT_JAR_FILENAME + "\"\r\n"
                        + "Content-Type: application/java-archive\r\n\r\n";

        String end = "\r\n--" + boundary + "--\r\n";

        byte[] startBytes = start.getBytes();
        byte[] endBytes = end.getBytes();

        byte[] full = new byte[startBytes.length + jarBytes.length + endBytes.length];

        System.arraycopy(startBytes, 0, full, 0, startBytes.length);
        System.arraycopy(jarBytes, 0, full, startBytes.length, jarBytes.length);
        System.arraycopy(endBytes, 0, full, startBytes.length + jarBytes.length, endBytes.length);

        HttpRequest req = HttpRequest.newBuilder()
                .uri(new URI(BASE_URL + "/management/v1/deployment/units/" + unitId + "/" + version + "?deployMode=ALL"))
                .header("Content-Type", "multipart/form-data; boundary=" + boundary)
                .POST(HttpRequest.BodyPublishers.ofByteArray(full))
                .build();

        HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());

        if (resp.statusCode() != 200 && resp.statusCode() != 409) {
            throw new RuntimeException("Deployment failed: " + resp.statusCode() + "\n" + resp.body());
        }

        // Wait for deployment to reach DEPLOYED status using polling
        await()
                .atMost(DEPLOYMENT_TIMEOUT_SECONDS, SECONDS)
                .pollInterval(200, MILLISECONDS)
                .until(() -> isDeployed(unitId, version));
    }

    /**
     * Undeploys the given deployment unit from the cluster and waits for it to be removed.
     *
     * @param unitId Deployment unit ID.
     * @param version Deployment version.
     * @throws Exception If undeployment fails.
     */
    public static void undeployUnit(String unitId, String version) throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(new URI(BASE_URL + "/management/v1/deployment/units/" + unitId + "/" + version))
                .DELETE()
                .build();

        HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());

        if (resp.statusCode() != 200 && resp.statusCode() != 404) {
            throw new RuntimeException("Undeploy failed: " + resp.statusCode() + "\n" + resp.body());
        }

        // Wait for deployment to be removed using polling
        await()
                .atMost(DEPLOYMENT_TIMEOUT_SECONDS, SECONDS)
                .pollInterval(200, MILLISECONDS)
                .until(() -> !deploymentExists(unitId, version));

        System.out.println("Unit successfully undeployed.");
    }

    /**
     * Checks if a deployment unit exists with DEPLOYED status. If it does not exist or is not deployed,
     * deploys the unit from the embedded classpath JAR and waits for it to reach DEPLOYED status.
     *
     * <p>The method uses polling to check the deployment status periodically until it reaches DEPLOYED state.
     * If the unit is already deployed, it skips the deployment process.</p>
     *
     * @param deploymentUnitName The name of the deployment unit to check and deploy.
     * @param deploymentUnitVersion The version of the deployment unit to check and deploy.
     * @throws Exception If an error occurs during the deployment process, such as a failure to deploy the unit.
     */
    public static void deployIfNotExist(String deploymentUnitName, String deploymentUnitVersion) throws Exception {
        if (isDeployed(deploymentUnitName, deploymentUnitVersion)) {
            System.out.println("Deployment unit already deployed. Skip deploy.");
        } else {
            System.out.println("Deployment unit not found or not in DEPLOYED state. Deploying...");
            deployUnit(deploymentUnitName, deploymentUnitVersion);
            System.out.println("Deployment completed: " + deploymentUnitName + " version " + deploymentUnitVersion + " is DEPLOYED.");
        }
    }
}
