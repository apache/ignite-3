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

package org.apache.ignite.internal.configuration.tree;

import org.jetbrains.annotations.Nullable;

/**
 * Polymorphic configuration node implementation.
 */
public abstract class PolymorphicInnerNode extends InnerNode {
    /**
     * Returns the name of the field that stores the type of the polymorphic configuration.
     *
     * @return Name of the field that stores the type of the polymorphic configuration.
     */
    public abstract String getPolymorphicTypeIdFieldName();

    /**
     * Returns current type of the polymorphic configuration.
     *
     * @return current type of the polymorphic configuration.
     */
    @Nullable public abstract String getPolymorphicTypeId();

    /**
     * Set the type of the polymorphic configuration with the reset of the fields of the previous type.
     *
     * @param typeId Type of the polymorphic configuration.
     */
    public abstract void setPolymorphicTypeId(String typeId);
}
