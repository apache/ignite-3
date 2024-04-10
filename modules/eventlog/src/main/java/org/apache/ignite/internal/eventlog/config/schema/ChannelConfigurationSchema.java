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

package org.apache.ignite.internal.eventlog.config.schema;

import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.InjectedName;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.internal.eventlog.api.IgniteEventType;

/** Channel configuration schema. */
@Config
public class ChannelConfigurationSchema {
    /** Channel name. This name is used to reference from sink configuration. */
    @InjectedName
    public String name;

    /** Channel enabled flag. */
    @Value(hasDefault = true)
    public boolean enabled = true;

    /** Event types that this channel should handle. Should be the types from {@link IgniteEventType}. */
    @ValidEventType
    @Value(hasDefault = true)
    public String[] events = {};
}
