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

import static java.util.Collections.emptySet;

import java.util.Collection;
import java.util.Set;
import org.apache.ignite.configuration.annotation.ConfigurationExtension;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.configuration.annotation.PolymorphicConfig;
import org.apache.ignite.configuration.validation.Validator;

/**
 * A module of configuration provided by a JAR file (or, in its source form, by a Maven/Gradle/... module).
 *
 * <p>Each configuration module only supplies configuration of only one {@link ConfigurationType}, so,
 * if a library needs to provide both node-local and cluster-wide configuration, it needs to supply
 * two ConfigurationModule instances.
 *
 * <p>Designed for integration with {@link java.util.ServiceLoader} mechanism, so ConfigurationModule instances
 * provided by a library are to be defined as services either in
 * {@code META-INF/services/org.apache.ignite.configuration.ConfigurationModule}, or in a {@code module-info.java}.
 *
 * <p>Supplies the following configuration components:
 * <ul>
 *     <li><b>rootKeys</b> ({@link RootKey} instances)</li>
 *     <li><b>validators</b> ({@link Validator} instances)</li>
 *     <li><b>schemaExtensions</b> (classes annotated with {@link ConfigurationExtension})</li>
 *     <li><b>polymorphicSchemaExtensions</b> (classes annotated with {@link PolymorphicConfig})</li>
 * </ul>
 *
 * @see ConfigurationType
 * @see RootKey
 * @see Validator
 * @see ConfigurationExtension
 * @see PolymorphicConfig
 */
public interface ConfigurationModule {
    /**
     * Type of the configuration provided by this module.
     *
     * @return configuration type
     */
    ConfigurationType type();

    /**
     * Returns keys of configuration roots provided by this module.
     *
     * @return root keys
     */
    default Collection<RootKey<?, ?, ?>> rootKeys() {
        return emptySet();
    }

    /**
     * Returns configuration validators provided by this module.
     *
     * @return configuration validators
     */
    default Set<Validator<?, ?>> validators() {
        return emptySet();
    }

    /**
     * Returns classes of schema extensions (annotated with {@link ConfigurationExtension})
     * provided by this module, including internal extensions.
     */
    default Collection<Class<?>> schemaExtensions() {
        return emptySet();
    }

    /**
     * Returns classes of polymorphic schema extensions (annotated with {@link PolymorphicConfig})
     * provided by this module.
     *
     * @return polymorphic schema extensions' classes
     */
    default Collection<Class<?>> polymorphicSchemaExtensions() {
        return emptySet();
    }

    /**
     * Patches the provided configuration with dynamic default values. This method is called
     * <ul>
     *     <li>for cluster-wide configuration on cluster initialization.</li>
     *     <li>for node-local configuration on the node start during the process of local configs reading.</li>
     * </ul>
     *
     * <p>Dynamic defaults are default values that are not specified in the configuration source,
     * but are added to the configuration on-the-fly, based on some external conditions.
     *
     * <p>For example, if the configuration contains a list of caches, and the user specifies
     * a list of caches in the source, then the defaults for the caches are not applied.
     * But if the user does not specify a list of caches, then the configuration module may add
     * a cache to the list based on some external conditions.
     *
     * <p>The default implementation of this method is a no-op.
     *
     * @param rootChange Root change.
     */
    default void patchConfigurationWithDynamicDefaults(SuperRootChange rootChange) {
        // No-op.
    }

    /**
     * Logic to perform configuration migration when Ignite version is upgraded. Main task - replace deprecated configuration values with
     * their non-deprecated cousins.
     *
     * <p>Typical implementation should look something like this:
     * <pre><code>
     *      var barValue = superRoot.viewRoot(KEY).foo().oldConfiguration().bar()
     *
     *      if (barValue != BAR_DEFAULT) { // Usually implies explicitly set value.
     *          superRoot.changeRoot(KEY).changeNewFoo().changeBar(barValue)
     *      }
     * </code></pre>
     *
     * @param superRootChange Super root change instance.
     */
    default void migrateDeprecatedConfigurations(SuperRootChange superRootChange) {
        // No-op.
    }

    /**
     * Returns a collection of prefixes, removed from configuration. Keys that match any of the prefixes
     * in this collection will be deleted.
     *
     * <p>Use {@code ignite.my.deleted.property} for regular deleted properties
     *
     * <p>{@code ignite.list.*.deletedProperty} - for named list elements. Arbitrarily nested named lists are supported
     *
     * @return A collection of prefixes of deleted keys.
     */
    default Collection<String> deletedPrefixes() {
        return emptySet();
    }
}
