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

package org.apache.ignite.configuration.internal.asm;

import org.apache.ignite.configuration.internal.DynamicConfiguration;
import org.apache.ignite.configuration.tree.InnerNode;

class SchemaClassesInfo {
    public final Class<?> schemaClass;

    public final String viewClassName;

    public final String changeClassName;

    public final String cfgClassName;

    public final String nodeClassName;

    public final String cfgImplClassName;

    public Class<? extends InnerNode> nodeClass;

    public Class<? extends DynamicConfiguration<?, ?>> cfgImplClass;

    SchemaClassesInfo(Class<?> schemaClass) {
        this.schemaClass = schemaClass;
        String schemaClassName = schemaClass.getPackageName() + "." + schemaClass.getSimpleName(); // Support inner classes.

        String prefix = schemaClassName.replaceAll("ConfigurationSchema$", "");

        viewClassName = prefix + "View";
        changeClassName = prefix + "Change";
        cfgClassName = prefix + "Configuration";

        nodeClassName = prefix + "Node";
        cfgImplClassName = prefix + "ConfigurationImpl";
    }
}
