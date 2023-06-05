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

import jakarta.inject.Singleton;
import org.apache.ignite.internal.cli.core.call.Call;
import org.apache.ignite.internal.cli.core.call.CallOutput;
import org.apache.ignite.internal.cli.core.call.DefaultCallOutput;
import org.apache.ignite.internal.cli.core.exception.IgniteCliApiException;
import org.apache.ignite.internal.cli.core.exception.UnitNotFoundException;
import org.apache.ignite.internal.cli.core.rest.ApiClientFactory;
import org.apache.ignite.internal.cli.core.style.component.MessageUiComponent;
import org.apache.ignite.internal.cli.core.style.element.UiElements;
import org.apache.ignite.rest.client.api.DeploymentApi;
import org.apache.ignite.rest.client.invoker.ApiException;

/** Call to undeploy a unit. */
@Singleton
public class UndeployUnitCall implements Call<UndeployUnitCallInput, String> {

    private final ApiClientFactory clientFactory;

    UndeployUnitCall(ApiClientFactory clientFactory) {
        this.clientFactory = clientFactory;
    }

    @Override
    public CallOutput<String> execute(UndeployUnitCallInput input) {
        try {
            DeploymentApi api = new DeploymentApi(clientFactory.getClient(input.clusterUrl()));
            api.undeployUnit(input.id(), input.version());

            return DefaultCallOutput.success(MessageUiComponent.from(UiElements.done()).render());
        } catch (ApiException e) {
            if (e.getCode() == 404) {
                return DefaultCallOutput.failure(new UnitNotFoundException(input.id(), input.version()));
            }

            return DefaultCallOutput.failure(new IgniteCliApiException(e, input.clusterUrl()));
        }
    }
}
