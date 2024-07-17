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
import java.util.List;
import org.apache.ignite.internal.cli.call.unit.ListUnitCall;
import org.apache.ignite.internal.cli.call.unit.ListUnitCallInput;
import org.apache.ignite.internal.cli.core.rest.ApiClientFactory;
import org.apache.ignite.rest.client.api.DeploymentApi;
import org.apache.ignite.rest.client.invoker.ApiException;
import org.apache.ignite.rest.client.model.UnitStatus;

/** List units on the cluster call. */
@Singleton
public class ClusterListUnitCall extends ListUnitCall {
    private final ApiClientFactory clientFactory;

    public ClusterListUnitCall(ApiClientFactory clientFactory) {
        this.clientFactory = clientFactory;
    }

    @Override
    protected List<UnitStatus> getStatuses(ListUnitCallInput input) throws ApiException {
        DeploymentApi api = new DeploymentApi(clientFactory.getClient(input.url()));
        if (input.unitId() != null) {
            return api.listClusterStatusesByUnit(input.unitId(), input.version(), input.statuses());
        } else {
            return api.listClusterStatuses(input.statuses());
        }
    }
}
