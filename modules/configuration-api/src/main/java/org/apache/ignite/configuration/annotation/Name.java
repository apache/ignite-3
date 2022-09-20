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

package org.apache.ignite.configuration.annotation;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * This annotation is used in conjunction with {@link ConfigValue} in case the nested schema contains a field with {@link InjectedName} in
 * order to be able to set different default values for the field with {@link InjectedName}.
 * <pre><code>
 * {@literal @}Config
 *  public class DataRegionConfigurationSchema {
 *      {@literal @InjectedName}
 *       public String name;
 *
 *      {@literal @}Value
 *       public long size;
 * }
 *
 * {@literal @}Config
 *  public class DataStorageConfigurationSchema {
 *      {@literal @}Name("defaultInMemory")
 *      {@literal @}ConfigValue
 *       public DataRegionConfigurationSchema defaultInMemoryRegion;
 *
 *      {@literal @}Name("defaultPersistent")
 *      {@literal @}ConfigValue
 *       public DataRegionConfigurationSchema defaultPersistentRegion;
 * }
 * </code></pre>
 *
 * @see ConfigValue
 * @see InjectedName
 */
@Target(FIELD)
@Retention(RUNTIME)
@Documented
public @interface Name {
    /**
     * Returns the default value for {@link InjectedName}.
     */
    String value();
}
