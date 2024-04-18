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

package org.apache.ignite.internal.catalog.commands;

import java.util.List;
import org.apache.ignite.internal.catalog.descriptors.CatalogSystemViewDescriptor.SystemViewType;

/**
 * A builder to construct create system view commands.
 *
 * <p>A builder is considered to be reusable, thus implementation have
 * to make sure invocation of {@link #build()} method doesn't cause any
 * side effects on builder's state or any object created by the same builder.
 */
public interface CreateSystemViewCommandBuilder {

    /**
     * Sets view name.
     *
     * @param name System view name. Name should neither be null nor blank.
     * @return this.
     */
    CreateSystemViewCommandBuilder name(String name);

    /**
     * Sets type of system view.
     *
     * @param systemViewType System view type. Type should not be null.
     * @return this.
     */
    CreateSystemViewCommandBuilder type(SystemViewType systemViewType);

    /**
     * Sets a list of columns.
     *
     * @param columns A list of columns. Column list should not be empty.
     * @return this.
     */
    CreateSystemViewCommandBuilder columns(List<ColumnParams> columns);

    /**
     * Creates an instance of create view command.
     *
     * @return An instance of create system view command.
     */
    CreateSystemViewCommand build();
}
