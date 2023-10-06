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

package org.apache.ignite.configuration;

import java.util.concurrent.CompletableFuture;

/**
 * Configuration defaults setter. This interface is used to set the default values for the cluster configuration that can not be set
 * in the configuration itself.
 */
public interface ConfigurationDefaultsSetter {
    /*
     * Sets the default values for the cluster configuration. This method is called when the cluster is started for the first time.
     *
     * @param hocon HOCON representation of the cluster configuration.
     * @return Future that is completed when the default values are set.
     */
    CompletableFuture<String> setDefaults(String hocon);
}
