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

package org.apache.ignite.internal.cli.call.cluster.unit;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import okhttp3.Call;
import okhttp3.MediaType;
import okhttp3.MultipartBody;
import okhttp3.Request;
import okhttp3.RequestBody;
import org.apache.ignite.rest.client.invoker.ApiCallback;
import org.apache.ignite.rest.client.invoker.ApiClient;
import org.apache.ignite.rest.client.invoker.ApiException;
import org.apache.ignite.rest.client.invoker.ApiResponse;
import org.apache.ignite.rest.client.invoker.Pair;
import org.apache.ignite.rest.client.invoker.ProgressRequestBody;
import org.apache.ignite.rest.client.model.DeployMode;
import org.jetbrains.annotations.Nullable;

/**
 * Temporary class for calling REST with list of files until underlying issue in the openapi-generator is fixed.
 * TODO https://issues.apache.org/jira/browse/IGNITE-19295
 */
public class DeployUnitClient {
    private final ApiClient apiClient;

    public DeployUnitClient(ApiClient apiClient) {
        this.apiClient = apiClient;
    }

    /**
     * Deploy unit.
     *
     * @param unitId The ID of the deployment unit.
     * @param unitContent The code to deploy.
     * @param unitVersion The version of the deployment unit.
     * @return {@code true} if the call succeeded.
     * @throws ApiException if fail to call.
     */
    public Boolean deployUnit(
            String unitId,
            List<File> unitContent,
            String unitVersion,
            DeployMode deployMode,
            List<String> initialNodes
    ) throws ApiException {
        Call call = deployUnitCall(unitId, unitContent, unitVersion, deployMode, initialNodes, null);
        ApiResponse<Boolean> response = apiClient.execute(call, Boolean.class);
        return response.getData();
    }

    /**
     * Deploy unit asynchronously.
     *
     * @param unitId The ID of the deployment unit.
     * @param unitContent The code to deploy.
     * @param unitVersion The version of the deployment unit.
     * @return Request call.
     */
    public Call deployUnitAsync(
            String unitId,
            List<File> unitContent,
            String unitVersion,
            @Nullable DeployMode deployMode,
            @Nullable List<String> initialNodes,
            ApiCallback<Boolean> callback
    ) {
        Call call = deployUnitCall(unitId, unitContent, unitVersion, deployMode, initialNodes, callback);
        apiClient.executeAsync(call, Boolean.class, callback);
        return call;
    }

    private Call deployUnitCall(
            String unitId,
            List<File> unitContent,
            String unitVersion,
            @Nullable DeployMode deployMode,
            @Nullable List<String> initialNodes,
            ApiCallback<Boolean> callback
    ) {
        StringBuilder url = new StringBuilder(apiClient.getBasePath());
        url
                .append("/management/v1/deployment/units")
                .append('/').append(apiClient.escapeString(unitId))
                .append('/').append(apiClient.escapeString(unitVersion));

        List<Pair> queryParams = new ArrayList<>();

        if (deployMode != null) {
            // parameterToPairs escape values while parameterToPair does not
            queryParams.addAll(apiClient.parameterToPairs("multi", "deployMode", List.of(deployMode)));
        }

        if (initialNodes != null) {
            queryParams.addAll(apiClient.parameterToPairs("multi", "initialNodes", initialNodes));
        }

        if (!queryParams.isEmpty()) {
            url.append(queryParams.stream()
                    .map(pair -> pair.getName() + "=" + pair.getValue())
                    .collect(Collectors.joining("&", "?", "")));
        }

        MultipartBody.Builder mpBuilder = new MultipartBody.Builder().setType(MultipartBody.FORM);
        for (File file : unitContent) {
            RequestBody requestBody = RequestBody.create(file, MediaType.parse("application/octet-stream"));
            mpBuilder.addFormDataPart("unitContent", file.getName(), requestBody);
        }
        MultipartBody body = mpBuilder.build();

        Request.Builder reqBuilder = new Request.Builder()
                .url(url.toString())
                .header("Accept", "application/json")
                .header("Content-Type", "multipart/form-data");

        if (callback != null) {
            ProgressRequestBody progressRequestBody = new ProgressRequestBody(body, callback);
            reqBuilder.tag(callback)
                    .post(progressRequestBody);
        } else {
            reqBuilder.post(body);
        }

        return apiClient.getHttpClient().newCall(reqBuilder.build());
    }
}
