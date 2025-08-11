package org.apache.ignite.example.code.deployment;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.Ignite;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.JobExecutionContext;

public class MyJob implements ComputeJob<String, String> {

    @Override
    public CompletableFuture<String> executeAsync(JobExecutionContext ctx, String word) {
        Ignite ignite = ctx.ignite();

        try (InputStream in = MyJob.class.getResourceAsStream("/org/apache/ignite/example/code/deployment/resources/script.txt")) {
            if (in == null) {
                throw new IllegalStateException("Resource not found: /org/apache/ignite/example/code/deployment/resources/script.txt");
            }
            String scriptContent = new String(in.readAllBytes(), StandardCharsets.UTF_8);

            String result = "Node: " + ignite.name()
                    + "\nArg: " + word
                    + "\nResource content:\n" + scriptContent;

            return CompletableFuture.completedFuture(result);
        } catch (Exception e) {

            throw new RuntimeException("Failed to load resource", e);
        }
    }
}

