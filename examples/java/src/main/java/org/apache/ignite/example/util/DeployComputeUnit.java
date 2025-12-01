package org.apache.ignite.example.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

public class DeployComputeUnit {

    private static final String BASE_URL = "http://localhost:10300";
    private static final HttpClient HTTP = HttpClient.newHttpClient();


    public static void buildJar(Path classesDir, Path jarPath) throws IOException {
        if (!Files.exists(classesDir)) {
            throw new IllegalArgumentException("Compiled classes not found: " + classesDir);
        }

        Files.createDirectories(jarPath.getParent());

        try (OutputStream fos = Files.newOutputStream(jarPath);
                JarOutputStream jar = new JarOutputStream(fos, createManifest())) {

            Files.walk(classesDir).filter(Files::isRegularFile).forEach(path -> {
                String entry = classesDir.relativize(path).toString().replace("\\", "/");
                try (InputStream is = Files.newInputStream(path)) {
                    jar.putNextEntry(new JarEntry(entry));
                    is.transferTo(jar);
                    jar.closeEntry();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }

        System.out.println("JAR built: " + jarPath);
    }

    private static Manifest createManifest() {
        Manifest m = new Manifest();
        m.getMainAttributes().putValue("Manifest-Version", "1.0");
        return m;
    }

    public static void deployUnitIfNeeded(String unitId, String version, Path jar) throws Exception {
        if (deploymentExists(unitId, version)) {
            System.out.println("Deployment unit already active. Skipping deployment.");
            return;
        }
        deployUnit(unitId, version, jar);
        System.out.println("Deployment completed.");
    }



    public static boolean deploymentExists(String unitId, String version) throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(new URI(BASE_URL + "/management/v1/deployment/cluster/units/" + unitId))
                .GET().build();

        HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
        return resp.statusCode() == 200 && resp.body().contains("\"version\":\"" + version + "\"");
    }

    public static void deployUnit(String unitId, String version, Path jar) throws Exception {
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

        Thread.sleep(500);

        if (resp.statusCode() != 200 && resp.statusCode() != 409) {
            throw new RuntimeException("Deployment failed: " + resp.statusCode() + "\n" + resp.body());
        }
    }

    public static void undeployUnit(String unitId, String version) throws Exception {
        // Call Ignite REST undeploy API
        HttpRequest req = HttpRequest.newBuilder()
                .uri(new URI(BASE_URL + "/management/v1/deployment/units/" + unitId + "/" + version))
                .DELETE()
                .build();

        HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());

        if (resp.statusCode() != 200 && resp.statusCode() != 404) {
            throw new RuntimeException("Undeploy failed: " + resp.statusCode() + "\n" + resp.body());
        }

        // Poll until gone
        for (int i = 0; i < 10; i++) {
            if (!deploymentExists(unitId, version)) {
                System.out.println("Unit successfully undeployed.");
                return;
            }
            Thread.sleep(300);
        }

        throw new RuntimeException("Undeploy timeout — unit still present.");
    }


    /**
     * Utility Method to Processes the commandline args if any and picks up two args 'runFromIDE' and 'jarPath'.
     * runFromIDE: Can be true or false. Specifies if the example is run from the project source if true. if false then it denotes the example is executed as a JAR
     * jarPath: If the 'runFromIDE' is false, then we need to explicitly specify the path to the JAR file containing the deployment unit code.
     * @param args
     * @return
     */
    public static Map<String, Object> processArguments(String[] args) {
        Map<String, Object> response = new HashMap<>();

        if(args == null){
            return response;
        }

        boolean runFromIDE = true;
        String jarPath = null;
        for (String arg : args) {

            if(arg.contains("runFromIDE")){
                String[] splitArgArr = arg.split("=");
                if(splitArgArr != null && splitArgArr.length == 2) {
                    runFromIDE = Boolean.parseBoolean(splitArgArr[1]);
                }else{
                    throw new RuntimeException(" 'runFromIDE' Argument not specified in the required format ");
                }
            }


            if(arg.contains("jarPath")){
                String[] splitArgArr = arg.split("=");
                if(splitArgArr != null && splitArgArr.length == 2) {
                    jarPath = splitArgArr[1];
                }else{
                    throw new RuntimeException(" 'jarPath' Argument not specified in the required format ");
                }
            }

        }


        response.put("runFromIDE", runFromIDE);
        response.put("jarPath", jarPath);

        return response;
    }
}