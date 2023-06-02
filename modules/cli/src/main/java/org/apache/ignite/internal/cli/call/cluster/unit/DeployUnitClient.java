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
import java.util.List;
import okhttp3.Call;
import okhttp3.MediaType;
import okhttp3.MultipartBody;
import okhttp3.Request;
import okhttp3.RequestBody;
import org.apache.ignite.rest.client.invoker.ApiCallback;
import org.apache.ignite.rest.client.invoker.ApiClient;
import org.apache.ignite.rest.client.invoker.ApiException;
import org.apache.ignite.rest.client.invoker.ApiResponse;
import org.apache.ignite.rest.client.invoker.ProgressRequestBody;

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
    public Boolean deployUnit(String unitId, List<File> unitContent, String unitVersion) throws ApiException {
        Call call = deployUnitCall(unitId, unitContent, unitVersion, null);
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
    public Call deployUnitAsync(String unitId, List<File> unitContent, String unitVersion, ApiCallback<Boolean> callback) {
        Call call = deployUnitCall(unitId, unitContent, unitVersion, callback);
        apiClient.executeAsync(call, Boolean.class, callback);
        return call;
    }

    private Call deployUnitCall(String unitId, List<File> unitContent, String unitVersion, ApiCallback<Boolean> callback) {
        String url = apiClient.getBasePath() + "/management/v1/deployment/units";

        MultipartBody.Builder mpBuilder = new MultipartBody.Builder().setType(MultipartBody.FORM);
        mpBuilder.addFormDataPart("unitId", unitId);
        mpBuilder.addFormDataPart("unitVersion", unitVersion);
        for (File file : unitContent) {
            RequestBody requestBody = RequestBody.create(file, MediaType.parse("application/octet-stream"));
            mpBuilder.addFormDataPart("unitContent", file.getName(), requestBody);
        }
        MultipartBody body = mpBuilder.build();

        Request.Builder reqBuilder = new Request.Builder()
                .url(url)
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
