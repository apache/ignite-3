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

package org.apache.ignite.internal.client;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.testframework.IgniteTestUtils;

/**
 * Tests that current Java client can work with all older server versions.
 */
public abstract class AbstractCurrentClientWithOldServerCompatibilityTest extends ClientCompatibilityTestBase {
    abstract String serverVersion();

    private static void activateCluster(int restPort) throws IOException {
        URL url = new URL("http://localhost:" + restPort + "/management/v1/cluster/init");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setDoOutput(true);

        String jsonInput = "{\"metaStorageNodes\": [\"defaultNode\"], \"clusterName\": \"myCluster\"}";

        try (OutputStream os = conn.getOutputStream()) {
            os.write(jsonInput.getBytes());
            os.flush();
        }

        int responseCode = conn.getResponseCode();

        if (responseCode != HttpURLConnection.HTTP_OK && responseCode != HttpURLConnection.HTTP_CREATED) {
            throw new IOException("Failed to activate cluster: " + responseCode + " " + conn.getResponseMessage());
        }

        conn.disconnect();
    }

    private static void waitForActivation(int clientPort) throws InterruptedException {
        IgniteTestUtils.waitForCondition(() -> {
            try (IgniteClient ignored = IgniteClient.builder()
                    .connectTimeout(500)
                    .addresses("localhost:" + clientPort)
                    .build()) {
                return true;
            } catch (Exception e) {
                return false;
            }
        }, 1_000, 20_000);
    }
}
