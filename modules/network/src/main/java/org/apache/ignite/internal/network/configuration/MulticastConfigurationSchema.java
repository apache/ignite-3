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

package org.apache.ignite.internal.network.configuration;

import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.validation.Range;

/** Configuration for multicast node finder. */
@Config
public class MulticastConfigurationSchema {
    @Value(hasDefault = true)
    @MulticastAddress
    public final String group = "228.1.2.4";

    @Value(hasDefault = true)
    @Range(min = 0, max = 65535)
    public final int port = 47400;

    @Value(hasDefault = true)
    @Range(min = 0)
    public final int discoveryResultWaitMillis = 500;

    @Value(hasDefault = true)
    @Range(min = -1, max = 255)
    public final int ttl = -1;
}
