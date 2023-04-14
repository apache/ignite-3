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


import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.internalSchemaExtensions;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.polymorphicSchemaExtensions;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.configuration.asm.ConfigurationAsmGenerator;
import org.apache.ignite.internal.configuration.tree.InnerNode;

/** Schema aware configuration generator. */
public class ConfigurationTreeGenerator implements ManuallyCloseable {

    private ConfigurationAsmGenerator generator = new ConfigurationAsmGenerator();

    private final Map<String, RootKey<?, ?>> rootKeys;

    /**
     * Constructor that takes a collection of root keys. Internal and polymorphic schema extensions are empty by default.
     *
     * @param rootKeys Root keys.
     */
    public ConfigurationTreeGenerator(Collection<RootKey<?, ?>> rootKeys) {
        this(rootKeys, Set.of(), Set.of());
    }

    /**
     * Constructor that takes a collection of root keys and a collection of internal schema extensions.
     *
     * @param rootKeys Root keys.
     * @param internalSchemaExtensions Internal schema extensions.
     * @param polymorphicSchemaExtensions Polymorphic schema extensions.
     */
    public ConfigurationTreeGenerator(
            Collection<RootKey<?, ?>> rootKeys,
            Collection<Class<?>> internalSchemaExtensions,
            Collection<Class<?>> polymorphicSchemaExtensions) {

        this.rootKeys = rootKeys.stream().collect(toMap(RootKey::key, identity()));

        Map<Class<?>, Set<Class<?>>> internalExtensions = internalSchemaExtensions(internalSchemaExtensions);
        Map<Class<?>, Set<Class<?>>> polymorphicExtensions = polymorphicSchemaExtensions(polymorphicSchemaExtensions);

        rootKeys.forEach(key -> generator.compileRootSchema(key.schemaClass(), internalExtensions, polymorphicExtensions));
    }

    /**
     * Creates a new instance of {@link SuperRoot} with all the roots created.
     *
     * @return New instance of {@link SuperRoot}.
     */
    public SuperRoot createSuperRoot() {
        assert generator != null : "ConfigurationTreeGenerator is already closed";

        SuperRoot superRoot = new SuperRoot(rootCreator());
        for (RootKey<?, ?> rootKey : rootKeys.values()) {
            superRoot.addRoot(rootKey, createRootNode(rootKey));
        }

        return superRoot;
    }

    /**
     * Creates new instance of {@code *Configuration} class corresponding to the given Configuration Schema.
     *
     * @param rootKey Root key of the configuration root.
     * @param changer Configuration changer instance to pass into constructor.
     * @return Configuration instance.
     */
    public DynamicConfiguration<?, ?> instantiateCfg(RootKey<?, ?> rootKey, DynamicConfigurationChanger changer) {
        assert generator != null : "ConfigurationTreeGenerator is already closed";

        return generator.instantiateCfg(rootKey, changer);
    }

    /**
     * Creates new instance of {@code *Node} class corresponding to the given Configuration Schema.
     *
     * @param schemaClass Configuration Schema class.
     * @return Node instance.
     */
    public synchronized InnerNode instantiateNode(Class<?> schemaClass) {
        assert generator != null : "ConfigurationTreeGenerator is already closed";

        return generator.instantiateNode(schemaClass);
    }

    /**
     * Creates new instance of root {@code *Node} class corresponding to the given Configuration Schema.
     */
    public InnerNode createRootNode(RootKey<?, ?> rootKey) {
        return instantiateNode(rootKey.schemaClass());
    }

    private Function<String, RootInnerNode> rootCreator() {
        return key -> {
            RootKey<?, ?> rootKey = rootKeys.get(key);

            return rootKey == null ? null : new RootInnerNode(rootKey, createRootNode(rootKey));
        };
    }

    @Override
    public void close() throws Exception {
        generator = null;
    }
}
