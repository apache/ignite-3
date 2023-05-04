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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Current rebalance mechanism based on the restriction, that the target zone has at most 1 table.
 * This restriction is temporary and must be fixed after the implementation of zone-based data distribution
 * instead the table-based.
 *
 * <p>So, this annotation must be place only on the {@link DistributionZoneConfiguration#replicas()} field to provide the check
 * for this restriction.
 */
// TODO: Remove under the IGNITE-19424
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface IsReplicasChangeAvailable {
}
