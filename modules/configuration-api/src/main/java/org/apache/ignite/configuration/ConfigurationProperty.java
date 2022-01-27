/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
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

package org.apache.ignite.configuration;

import org.apache.ignite.configuration.notifications.ConfigurationListener;

/**
 * Base interface for configuration.
 *
 * @param <VIEWT> Type of the value.
 */
public interface ConfigurationProperty<VIEWT> {
    /**
     * Get key of this node.
     */
    String key();

    /**
     * Get value of this property.
     */
    VIEWT value();

    /**
     * Adds configuration values listener.
     *
     * <p>NOTE: Listener will be called starting from the next notification of all configuration listeners.
     *
     * @param listener Listener.
     */
    void listen(ConfigurationListener<VIEWT> listener);

    /**
     * Removes configuration values listener.
     *
     * <p>NOTE: This method introduces unpredictable behavior at the moment, because the final behavior of this method is unclear.
     * Should the listener be removed immediately or only on the next notification? We'll fix it later if there's a problem.
     *
     * @param listener Listener.
     */
    void stopListen(ConfigurationListener<VIEWT> listener);
}
