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

package org.apache.ignite.internal.cli.call.unit;

import jakarta.inject.Singleton;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.internal.cli.core.call.Call;
import org.apache.ignite.internal.cli.core.call.CallOutput;
import org.apache.ignite.internal.cli.core.call.DefaultCallOutput;
import org.apache.ignite.internal.cli.core.call.UrlCallInput;
import org.apache.ignite.internal.cli.core.exception.IgniteCliApiException;
import org.apache.ignite.internal.cli.core.rest.ApiClientFactory;
import org.apache.ignite.rest.client.api.DeploymentApi;
import org.apache.ignite.rest.client.invoker.ApiException;
import org.apache.ignite.rest.client.model.UnitStatus;

/** List units call. */
@Singleton
public class ListUnitCall implements Call<UrlCallInput, List<UnitStatusRecord>> {

    private final ApiClientFactory clientFactory;

    public ListUnitCall(ApiClientFactory clientFactory) {
        this.clientFactory = clientFactory;
    }

    @Override
    public CallOutput<List<UnitStatusRecord>> execute(UrlCallInput input) {
        try {
            List<UnitStatus> units = new DeploymentApi(clientFactory.getClient(input.getUrl())).units();
            if (units.isEmpty()) {
                return DefaultCallOutput.empty();
            }
            return DefaultCallOutput.success(
                    units.stream()
                            .map(this::toRecord)
                            .collect(Collectors.toList())
            );
        } catch (ApiException e) {
            return DefaultCallOutput.failure(new IgniteCliApiException(e, input.getUrl()));
        }
    }

    private UnitStatusRecord toRecord(UnitStatus unitStatus) {
        return new UnitStatusRecord(
                unitStatus.getId(),
                unitStatus.getVersionToConsistentIds()
        );
    }
}
