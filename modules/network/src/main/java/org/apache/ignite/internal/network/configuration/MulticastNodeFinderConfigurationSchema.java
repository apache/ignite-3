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

import static org.apache.ignite.internal.network.MulticastNodeFinder.MAX_TTL;
import static org.apache.ignite.internal.network.MulticastNodeFinder.UNSPECIFIED_TTL;

import org.apache.ignite.configuration.annotation.PolymorphicConfigInstance;
import org.apache.ignite.configuration.annotation.PublicName;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.validation.Range;
import org.apache.ignite.internal.network.MulticastNodeFinder;

/** Configuration for multicast node finder. */
@PolymorphicConfigInstance(MulticastNodeFinderConfigurationSchema.TYPE)
public class MulticastNodeFinderConfigurationSchema extends NodeFinderConfigurationSchema {
    public static final String TYPE = "MULTICAST";

    /** Address to use for multicast requests. */
    @Value(hasDefault = true)
    @MulticastAddress
    public String group = "239.192.0.0";

    /** Port to use for multicast requests. */
    @Value(hasDefault = true)
    @Range(min = 1, max = 65535)
    public int port = 47401;

    /** Time to wait for multicast responses. */
    @Value(hasDefault = true)
    @Range(min = 0)
    @PublicName(legacyNames = "resultWaitTime")
    public int resultWaitTimeMillis = 1000;

    /** Time to live for multicast packets. Value {@link MulticastNodeFinder#UNSPECIFIED_TTL} corresponds to system default value. */
    @Value(hasDefault = true)
    @Range(min = UNSPECIFIED_TTL, max = MAX_TTL)
    public int ttl = UNSPECIFIED_TTL;
}
