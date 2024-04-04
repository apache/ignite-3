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

import org.apache.ignite.configuration.annotation.PolymorphicConfigInstance;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.validation.OneOf;

/** Configuration schema for java logger sink. */
@PolymorphicConfigInstance(LogSinkConfigurationSchema.POLYMORPHIC_ID)
public class LogSinkConfigurationSchema extends SinkConfigurationSchema {
    public static final String POLYMORPHIC_ID = "log";

    /**
     * The criteria for the logger. In other words, the name of the logger. This name should be used to configure the logger in the logging
     * framework.
     */
    @Value(hasDefault = true)
    public String criteria = "EventLog";

    /** The logging level. */
    @OneOf({"ALL", "TRACE", "DEBUG", "INFO", "WARNING", "ERROR", "OFF"})
    @Value(hasDefault = true)
    public String level = "INFO";

    /** The format of the log message. */
    @OneOf("JSON")
    @Value(hasDefault = true)
    public String format = "JSON";
}
