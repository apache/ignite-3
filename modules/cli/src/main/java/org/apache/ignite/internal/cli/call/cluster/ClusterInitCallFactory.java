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

package org.apache.ignite.internal.cli.call.cluster;

import jakarta.inject.Singleton;
import org.apache.ignite.internal.cli.core.call.AsyncCall;
import org.apache.ignite.internal.cli.core.call.AsyncCallFactory;
import org.apache.ignite.internal.cli.core.call.ProgressTracker;
import org.apache.ignite.internal.cli.core.rest.ApiClientFactory;

/** Factory for {@link ClusterInitCall}. */
@Singleton
public class ClusterInitCallFactory implements AsyncCallFactory<ClusterInitCallInput, String> {

    private final ApiClientFactory factory;

    public ClusterInitCallFactory(ApiClientFactory factory) {
        this.factory = factory;
    }

    @Override
    public AsyncCall<ClusterInitCallInput, String> create(ProgressTracker tracker) {
        return new ClusterInitCall(tracker, factory);
    }
}
