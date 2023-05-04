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

package org.apache.ignite.internal.distributionzones.configuration;

import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.configuration.validation.Validator;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.schema.configuration.TablesView;

/**
 * Validator for {@link DistributionZoneConfiguration} replicas configuration.
 */
// TODO: Remove under the IGNITE-19424
public class RebalanceValidator implements Validator<IsReplicasChangeAvailable, Integer> {
    /** Static instance. */
    public static final RebalanceValidator INSTANCE = new RebalanceValidator();

    /** {@inheritDoc} */
    @Override
    public void validate(IsReplicasChangeAvailable annotation, ValidationContext<Integer> ctx) {
        assert (ctx.getNewOwner() instanceof DistributionZoneView) : ctx.currentKey();

        TablesView tablesView = ctx.getNewRoot(TablesConfiguration.KEY);

        int zoneId = ((DistributionZoneView) ctx.getNewOwner()).zoneId();

        if (ctx.getOldValue() != null
                && !ctx.getOldValue().equals(ctx.getNewValue())
                && tablesView.tables().stream().filter(t -> t.zoneId() == zoneId).count() > 1) {
            ctx.addIssue(
                    new ValidationIssue(
                            ctx.currentKey(),
                            String.format("Change the number of replicas fot the zone with the > 1 tables is not supported yet")));
        }
    }
}
