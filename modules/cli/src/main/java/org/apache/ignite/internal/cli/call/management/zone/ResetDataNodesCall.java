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

package org.apache.ignite.internal.cli.call.management.zone;

import jakarta.inject.Singleton;
import java.util.Collections;
import org.apache.ignite.internal.cli.core.call.Call;
import org.apache.ignite.internal.cli.core.call.CallOutput;
import org.apache.ignite.internal.cli.core.call.DefaultCallOutput;
import org.apache.ignite.internal.cli.core.exception.IgniteCliApiException;
import org.apache.ignite.internal.cli.core.rest.ApiClientFactory;
import org.apache.ignite.internal.cli.core.style.component.MessageUiComponent;
import org.apache.ignite.internal.cli.core.style.element.UiElements;
import org.apache.ignite.rest.client.api.DataNodesApi;
import org.apache.ignite.rest.client.invoker.ApiException;

/** Call to reset data nodes for distribution zones. */
@Singleton
public class ResetDataNodesCall implements Call<ResetDataNodesCallInput, String> {

    private final ApiClientFactory clientFactory;

    ResetDataNodesCall(ApiClientFactory clientFactory) {
        this.clientFactory = clientFactory;
    }

    @Override
    public CallOutput<String> execute(ResetDataNodesCallInput input) {
        try {
            DataNodesApi api = new DataNodesApi(clientFactory.getClient(input.clusterUrl()));
            // If no zone names specified, pass an empty list (which means reset all zones).
            api.resetDataNodesForZones(input.zoneNames() == null ? Collections.emptyList() : input.zoneNames());

            return DefaultCallOutput.success(MessageUiComponent.from(UiElements.done()).render());
        } catch (ApiException e) {
            return DefaultCallOutput.failure(new IgniteCliApiException(e, input.clusterUrl()));
        }
    }
}
