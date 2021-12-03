/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.configuration.schemas.table;

import static org.apache.ignite.configuration.schemas.table.TableIndexConfigurationSchema.PARTIAL_INDEX_TYPE;

import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.PolymorphicConfigInstance;
import org.apache.ignite.configuration.annotation.Value;

/**
 * Schema for the Partial Index configuration.
 */
@PolymorphicConfigInstance(PARTIAL_INDEX_TYPE)
public class PartialIndexConfigurationSchema extends TableIndexConfigurationSchema {
    /** Expression for PartialIndex: PARTIAL indexes. */
    @Value
    public String expr;

    /** Columns configuration for sorted indexes. */
    @NamedConfigValue
    public IndexColumnConfigurationSchema columns;
}
