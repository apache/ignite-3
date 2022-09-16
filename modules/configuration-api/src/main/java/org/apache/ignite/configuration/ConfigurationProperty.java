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
     * <p>NOTE: If this method is called from another listener, then it is guaranteed to be called starting from the next configuration
     * update only.
     *
     * @param listener Listener.
     */
    void listen(ConfigurationListener<VIEWT> listener);

    /**
     * Removes configuration values listener.
     *
     * <p>NOTE: Unpredictable behavior if the method is called inside other listeners.
     *
     * @param listener Listener.
     */
    void stopListen(ConfigurationListener<VIEWT> listener);
}
