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
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for deploying Ignite compute units.
 * <p>
 * Note: The deployment unit JAR is now built at compile time via the deploymentUnitJar Gradle task,
 * not at runtime. This eliminates the need for runtime JAR building.
 * </p>
 */
public class DeployComputeUnit {

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

        System.out.println("[DEBUG] Checking deployment status for " + unitId + " version " + version);
        System.out.println("[DEBUG] HTTP Status: " + resp.statusCode());
        System.out.println("[DEBUG] Response body: " + resp.body());

        if (resp.statusCode() == 404) {
            // Unit doesn't exist yet
            System.out.println("[DEBUG] Unit not found (404)");
            return false;
        }

        if (resp.statusCode() != 200) {
            throw new RuntimeException("Failed to check deployment status. HTTP " + resp.statusCode() + ": " + resp.body());
        }

        // Parse JSON response - the API returns a Collection<UnitStatus>
        JsonNode root = OBJECT_MAPPER.readTree(resp.body());

        System.out.println("[DEBUG] Parsed JSON root: " + root);
        System.out.println("[DEBUG] Is array: " + root.isArray() + ", Is object: " + root.isObject());

        // Handle empty response (unit exists but no matching status)
        if (root.isArray() && root.isEmpty()) {
            System.out.println("[DEBUG] Empty array response");
            return false;
        }

        // The response is an array of UnitStatus objects
        if (!root.isArray()) {
            throw new RuntimeException("Unexpected response format. Expected array, got: " + root);
        }

        // Check if any node has this version deployed
        for (JsonNode unitStatus : root) {
            System.out.println("[DEBUG] Processing UnitStatus: " + unitStatus);
            JsonNode versionToStatus = unitStatus.path("versionToStatus");
            System.out.println("[DEBUG] versionToStatus: " + versionToStatus);

            if (versionToStatus.isArray()) {
                for (JsonNode versionStatus : versionToStatus) {
                    String versionValue = versionStatus.path("version").asText();
                    String statusValue = versionStatus.path("status").asText();

                    System.out.println("[DEBUG] Found version: " + versionValue + ", status: " + statusValue);

                    if (version.equals(versionValue) && "DEPLOYED".equals(statusValue)) {
                        System.out.println("[DEBUG] MATCH FOUND - Deployment is ready!");
                        return true;
                    }
                }
            }
        }

        System.out.println("[DEBUG] No matching DEPLOYED status found");
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
     * @param unitId Deployment unit ID.
     * @param version Deployment version.
     * @param jar Path to the JAR file to upload.
     * @throws Exception If deployment fails.
     */
    private static void deployUnit(String unitId, String version, Path jar) throws Exception {
        String boundary = "igniteBoundary";

        byte[] jarBytes = Files.readAllBytes(jar);

        String start =
                "--" + boundary + "\r\n" +
                        "Content-Disposition: form-data; name=\"unitContent\"; filename=\"" + jar.getFileName() + "\"\r\n" +
                        "Content-Type: application/java-archive\r\n\r\n";

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
     * Processes command-line arguments for:
     * <ul>
     *   <li><b>runFromIDE</b> – whether the example runs from source</li>
     *   <li><b>jarPath</b> – path to external JAR when run outside IDE</li>
     * </ul>
     *
     * @param args Command-line arguments (may be null).
     * @return Map with keys "runFromIDE" and "jarPath".
     */
    public static Map<String, Object> processArguments(String[] args) {
        Map<String, Object> response = new HashMap<>();

        if (args == null) {
            return response;
        }

        boolean runFromIDE = true;
        String jarPath = null;

        for (String arg : args) {

            if (arg.contains("runFromIDE")) {
                String[] splitArgArr = arg.split("=");
                if (splitArgArr != null && splitArgArr.length == 2) {
                    runFromIDE = Boolean.parseBoolean(splitArgArr[1]);
                } else {
                    throw new RuntimeException(" 'runFromIDE' argument not specified in the required format ");
                }
            }

            if (arg.contains("jarPath")) {
                String[] splitArgArr = arg.split("=");
                if (splitArgArr != null && splitArgArr.length == 2) {
                    jarPath = splitArgArr[1];
                } else {
                    throw new RuntimeException(" 'jarPath' argument not specified in the required format ");
                }
            }
        }

        response.put("runFromIDE", runFromIDE);
        response.put("jarPath", jarPath);

        return response;
    }

    /**
     * Checks if a deployment unit exists with DEPLOYED status. If it does not exist or is not deployed,
     * deploys the unit and waits for it to reach DEPLOYED status.
     *
     * <p>The method uses polling to check the deployment status periodically until it reaches DEPLOYED state.
     * If the unit is already deployed, it skips the deployment process.</p>
     *
     * @param deploymentUnitName The name of the deployment unit to check and deploy.
     * @param deploymentUnitVersion The version of the deployment unit to check and deploy.
     * @param jarPath The file path to the JAR file that will be used for deployment, if necessary.
     * @throws Exception If an error occurs during the deployment process, such as a failure to deploy the unit.
     */
    public static void deployIfNotExist(String deploymentUnitName, String deploymentUnitVersion, Path jarPath) throws Exception {
        if (isDeployed(deploymentUnitName, deploymentUnitVersion)) {
            System.out.println("Deployment unit already deployed. Skip deploy.");
        } else {
            System.out.println("Deployment unit not found or not in DEPLOYED state. Deploying...");
            deployUnit(deploymentUnitName, deploymentUnitVersion, jarPath);
            System.out.println("Deployment completed: " + deploymentUnitName + " version " + deploymentUnitVersion + " is DEPLOYED.");
        }
    }
}
