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

package org.apache.ignite.internal.configuration.asm;

import org.apache.ignite.internal.configuration.DynamicConfiguration;
import org.apache.ignite.internal.configuration.tree.InnerNode;

/**
 * Class to cache compiled classes and hold precalculated names to reference other existing classes.
 */
class SchemaClassesInfo {
    /** Configuration class name postfix. */
    public static final String CONFIGURATION_CLASS_POSTFIX = "Configuration";

    /** Configuration Schema class. */
    public final Class<?> schemaClass;

    /** Class name for the VIEW class. */
    public final String viewClassName;

    /** Class name for the CHANGE class. */
    public final String changeClassName;

    /** Class name for the Configuration class. */
    public final String cfgClassName;

    /** Class name for the Node class. */
    public final String nodeClassName;

    /** Class name for the Configuration Impl class. */
    public final String cfgImplClassName;

    /** Node class instance. */
    public Class<? extends InnerNode> nodeClass;

    /** Configuration Impl class instance. */
    public Class<? extends DynamicConfiguration<?, ?>> cfgImplClass;

    /**
     * Constructor.
     *
     * @param schemaClass Configuration Schema class instance.
     */
    SchemaClassesInfo(Class<?> schemaClass) {
        this.schemaClass = schemaClass;

        String prefix = prefix(schemaClass);

        viewClassName = prefix + "View";
        changeClassName = prefix + "Change";
        cfgClassName = prefix + CONFIGURATION_CLASS_POSTFIX;

        nodeClassName = prefix + "Node";
        cfgImplClassName = prefix + "ConfigurationImpl";
    }

    /**
     * Get the prefix for inner classes.
     * <p/>
     * Example: org.apache.ignite.NodeConfigurationSchema -> org.apache.ignite.Node
     *
     * @param schemaCls Configuration schema class.
     * @return Prefix for inner classes.
     */
    static String prefix(Class<?> schemaCls) {
        String schemaClassName = schemaCls.getPackageName() + "." + schemaCls.getSimpleName();

        return schemaClassName.replaceAll("ConfigurationSchema$", "");
    }
}
