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

import org.apache.ignite.configuration.annotation.InjectedName;
import org.apache.ignite.configuration.annotation.PolymorphicConfig;
import org.apache.ignite.configuration.annotation.PolymorphicId;
import org.apache.ignite.configuration.annotation.Value;


/** Configuration schema for sink. */
@PolymorphicConfig
public class SinkConfigurationSchema {
    /** The type of the sink that is used to identify the type: log, webhook, kafka. */
    @PolymorphicId(hasDefault = true)
    public String type = LogSinkConfigurationSchema.POLYMORPHIC_ID;

    /** The name of the sink. */
    @InjectedName
    public String name;

    /** The channel to which the sink is connected. Should be one of existing channels. */
    @Value(hasDefault = true)
    public String channel = "";
}
