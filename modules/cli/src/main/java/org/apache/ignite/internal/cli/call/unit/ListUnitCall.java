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

import java.util.List;
import org.apache.ignite.internal.cli.core.call.Call;
import org.apache.ignite.internal.cli.core.call.CallOutput;
import org.apache.ignite.internal.cli.core.call.DefaultCallOutput;
import org.apache.ignite.internal.cli.core.exception.IgniteCliApiException;
import org.apache.ignite.rest.client.invoker.ApiException;
import org.apache.ignite.rest.client.model.UnitStatus;

/** List units call. */
public abstract class ListUnitCall implements Call<ListUnitCallInput, List<UnitStatus>> {

    @Override
    public CallOutput<List<UnitStatus>> execute(ListUnitCallInput input) {
        try {
            List<UnitStatus> units = getStatuses(input);
            if (units.isEmpty()) {
                return DefaultCallOutput.empty();
            }
            return DefaultCallOutput.success(units);
        } catch (ApiException e) {
            return DefaultCallOutput.failure(new IgniteCliApiException(e, input.url()));
        }
    }

    protected abstract List<UnitStatus> getStatuses(ListUnitCallInput input) throws ApiException;
}
