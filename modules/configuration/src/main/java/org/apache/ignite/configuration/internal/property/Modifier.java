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

package org.apache.ignite.configuration.internal.property;

import org.apache.ignite.configuration.internal.DynamicConfiguration;

/**
 * Interface for configuration nodes and leafs.
 */
public interface Modifier<T, INIT, CHANGE> {

    /**
     * Get key of this node.
     * @return Key.
     */
    String key();

    /**
     * Get VIEW object of this node.
     * @return VIEW object.
     */
    T toView();

    /**
     * Change this configuration node value.
     * @param change CHANGE object.
     */
    void change(CHANGE change);

    /**
     * Change this configuration node value, but without validation.
     * FIXME: this is a necessary evil, but this should'n be accessed from outside of the configurator.
     * @param change CHANGE object.
     */
    void changeWithoutValidation(CHANGE change);

    /**
     * Initialize this configuration node with value.
     * @param init INIT object.
     */
    void init(INIT init);

    /**
     * Initialize this configuration node with value, but without validation.
     * FIXME: this is a necessary evil, but this should'n be accessed from outside of the configurator.
     * @param init INIT object.
     */
    void initWithoutValidation(INIT init);

    /**
     * Validate this configuration node against old configuration root thus comparing new configuration "snapshot"
     * with a previous one.
     * @param oldRoot Old configuration root.
     */
    void validate(DynamicConfiguration<?, ?, ?> oldRoot);
}
