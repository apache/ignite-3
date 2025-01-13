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

package org.apache.ignite.internal.configuration;

import org.apache.ignite.configuration.ConfigurationModule;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.InjectedName;
import org.apache.ignite.configuration.annotation.InjectedValue;
import org.apache.ignite.configuration.validation.CamelCaseKeys;
import org.apache.ignite.internal.configuration.validation.LongNumberSystemPropertyValueValidator;

/**
 * System property configuration schema.
 *
 * <p>For the property name format, see {@link CamelCaseKeys}.</p>
 *
 * <p>To add validators for system property values:</p>
 * <ul>
 *     <li>Create a validator, for {@link LongNumberSystemPropertyValueValidator}.</li>
 *     <li>Add it to {@link ConfigurationModule#validators}, to the module where it is needed, for example, the metastorage module. Also
 *     note that validators for local and distributed properties must be in different {@link ConfigurationModule} of the module.</li>
 * </ul>
 */
@Config
public class SystemPropertyConfigurationSchema {
    @InjectedName
    public String name;

    @InjectedValue
    public String propertyValue;
}
