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

package org.apache.ignite.example.code.deployment;

import java.io.OutputStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.JobExecutionContext;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

//tag::load-script[]
public class MyJob implements ComputeJob<String, String> {
    @Override
    public CompletableFuture<String> executeAsync(JobExecutionContext ctx, String arg) {
        Ignite ignite = ctx.ignite();

        /** Full path to the script we want to run */
        final String resPath = "/org/apache/ignite/example/code/deployment/resources/script.sh";

        try (InputStream in = MyJob.class.getResourceAsStream(resPath)) {
            if (in == null) {
                throw new IllegalStateException("Resource not found: " + resPath);
            }

            byte[] script = in.readAllBytes();

            Process p = new ProcessBuilder("sh", "-s", "--", arg)
                    .redirectErrorStream(true)
                    .start();

            try (OutputStream os = p.getOutputStream()) {
                os.write(script);
            }

            String out;
            try (InputStream procOut = p.getInputStream()) {
                out = new String(procOut.readAllBytes(), StandardCharsets.UTF_8).strip();
            }

            int exit = p.waitFor();
            if (exit != 0) {
                throw new RuntimeException("Script exited with code " + exit + ":\n" + out);
            }

            String result = "Node: " + ignite.name()
                    + "\nArg: " + arg
                    + "\nScript output:\n" + out;

            return CompletableFuture.completedFuture(result);
        } catch (Exception e) {
            throw new RuntimeException("Failed to run script", e);
        }
    }
}
//end::load-script[]