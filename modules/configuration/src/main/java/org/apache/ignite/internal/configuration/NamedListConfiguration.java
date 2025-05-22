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

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiFunction;
import org.apache.ignite.configuration.ConfigurationProperty;
import org.apache.ignite.configuration.NamedConfigurationTree;
import org.apache.ignite.configuration.NamedListChange;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.apache.ignite.internal.configuration.direct.DirectNamedListProxy;
import org.apache.ignite.internal.configuration.direct.DirectPropertyProxy;
import org.apache.ignite.internal.configuration.direct.KeyPathNode;
import org.apache.ignite.internal.configuration.tree.NamedListNode;
import org.jetbrains.annotations.Nullable;

/**
 * Named configuration wrapper.
 */
public class NamedListConfiguration<T extends ConfigurationProperty<VIEWT>, VIEWT, CHANGET extends VIEWT>
        extends DynamicConfiguration<NamedListView<VIEWT>, NamedListChange<VIEWT, CHANGET>>
        implements NamedConfigurationTree<T, VIEWT, CHANGET> {
    /** Listeners of property update. */
    private final ConfigurationListenerHolder<ConfigurationNamedListListener<VIEWT>> extendedListeners =
            new ConfigurationListenerHolder<>();

    /** Creator of named configuration. */
    private final BiFunction<List<String>, String, T> cfgCreator;

    /** Creator of direct proxies for inner nodes. */
    private BiFunction<List<KeyPathNode>, DynamicConfigurationChanger, T> directProxyCreator;

    /** Placeholder to add listeners for any configuration. */
    private final T anyConfig;

    /** Notification cache. */
    private volatile Map<String, T> notificationCache = Collections.emptyMap();

    /**
     * Constructor.
     *
     * @param prefix Configuration prefix.
     * @param key Configuration key.
     * @param rootKey Root key.
     * @param changer Configuration changer.
     * @param listenOnly Only adding listeners mode, without the ability to get or update the property value.
     * @param cfgCreator Underlying configuration creator function.
     * @param directProxyCreator Creator for direct proxies of elements.
     *      Required for {@link DirectPropertyProxy#DirectPropertyProxy(List, DynamicConfigurationChanger)}.
     * @param anyConfig Placeholder to add listeners for any configuration.
     */
    public NamedListConfiguration(
            List<String> prefix,
            String key,
            RootKey<?, ?, ?> rootKey,
            DynamicConfigurationChanger changer,
            boolean listenOnly,
            BiFunction<List<String>, String, T> cfgCreator,
            BiFunction<List<KeyPathNode>, DynamicConfigurationChanger, T> directProxyCreator,
            T anyConfig
    ) {
        super(prefix, key, rootKey, changer, listenOnly);

        this.cfgCreator = cfgCreator;
        this.directProxyCreator = directProxyCreator;
        this.anyConfig = anyConfig;
    }

    /** {@inheritDoc} */
    @Override
    @Nullable
    public T get(String name) {
        DynamicConfiguration<?, ?> config = getConfig(name);

        return config == null ? null : (T) config.specificConfigTree();
    }

    @Override
    @Nullable
    public T get(UUID internalId) {
        var value = (NamedListNode<?>) value();

        return get(value.keyByInternalId(internalId));
    }

    /**
     * Get a raw named config.
     *
     * @param name Name.
     * @return Raw config.
     */
    @Nullable
    public DynamicConfiguration<?, ?> getConfig(String name) {
        refreshValue();

        return (DynamicConfiguration<?, ?>) members.get(name);
    }

    @Override
    public List<UUID> internalIds() {
        NamedListView<VIEWT> value = value();

        return List.copyOf(((NamedListNode<?>) value).internalIds());
    }

    /** {@inheritDoc} */
    @Override
    protected synchronized void beforeRefreshValue(
            NamedListView<VIEWT> newValue,
            @Nullable NamedListView<VIEWT> oldValue
    ) {
        Map<String, ConfigurationProperty<?>> oldValues = this.members;
        Map<String, ConfigurationProperty<?>> newValues = new LinkedHashMap<>();

        for (String key : newValue.namedListKeys()) {
            ConfigurationProperty<?> oldElement = oldValues.get(key);

            if (oldElement != null) {
                newValues.put(key, oldElement);
            } else {
                newValues.put(key, cfgCreator.apply(keys, key));
            }
        }

        this.members = newValues;
    }

    /** {@inheritDoc} */
    @Override
    public Map<String, ConfigurationProperty<?>> touchMembers() {
        Map<String, T> res = notificationCache;

        refreshValue();

        notificationCache = (Map<String, T>) members;

        return Collections.unmodifiableMap(res);
    }

    /** {@inheritDoc} */
    @Override
    public void listenElements(ConfigurationNamedListListener<VIEWT> listener) {
        extendedListeners.addListener(listener, changer.notificationCount());
    }

    /** {@inheritDoc} */
    @Override
    public void stopListenElements(ConfigurationNamedListListener<VIEWT> listener) {
        extendedListeners.removeListener(listener);
    }

    /**
     * Returns an iterator of the listeners for the {@code notificationNumber} (were added for and before it).
     *
     * <p>NOTE: {@link Iterator#remove} - not supported.
     *
     * @param notificationNumber Configuration notification listener number.
     */
    public Iterator<ConfigurationNamedListListener<VIEWT>> extendedListeners(long notificationNumber) {
        return extendedListeners.listeners(notificationNumber);
    }

    /** {@inheritDoc} */
    @Override
    public T any() {
        return anyConfig;
    }

    /** {@inheritDoc} */
    @Override
    public Class<?> configType() {
        throw new UnsupportedOperationException("Not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public Class<?> @Nullable [] extensionConfigTypes() {
        throw new UnsupportedOperationException("Not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Class<?> polymorphicInstanceConfigType() {
        throw new UnsupportedOperationException("Not supported.");
    }

    /** {@inheritDoc} */
    @Override
    public NamedListView<VIEWT> value() {
        return refreshValue();
    }

    /** {@inheritDoc} */
    @Override
    @SuppressWarnings("unchecked")
    public NamedConfigurationTree<T, VIEWT, CHANGET> directProxy() {
        if (listenOnly) {
            throw listenOnlyException();
        }

        assert directProxyCreator != null;

        return new DirectNamedListProxy<>(keyPath(), changer, directProxyCreator);
    }
}
