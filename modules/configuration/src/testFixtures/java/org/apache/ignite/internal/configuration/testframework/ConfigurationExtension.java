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

package org.apache.ignite.internal.configuration.testframework;

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.apache.ignite.configuration.annotation.ConfigurationType.LOCAL;
import static org.apache.ignite.internal.configuration.notifications.ConfigurationNotifier.notifyListeners;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.findEx;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.internalSchemaExtensions;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.polymorphicSchemaExtensions;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.touch;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigObject;
import java.lang.reflect.Field;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.internal.configuration.ConfigurationListenerHolder;
import org.apache.ignite.internal.configuration.DynamicConfiguration;
import org.apache.ignite.internal.configuration.DynamicConfigurationChanger;
import org.apache.ignite.internal.configuration.RootInnerNode;
import org.apache.ignite.internal.configuration.SuperRoot;
import org.apache.ignite.internal.configuration.asm.ConfigurationAsmGenerator;
import org.apache.ignite.internal.configuration.direct.KeyPathNode;
import org.apache.ignite.internal.configuration.hocon.HoconConverter;
import org.apache.ignite.internal.configuration.notifications.ConfigurationStorageRevisionListener;
import org.apache.ignite.internal.configuration.notifications.ConfigurationStorageRevisionListenerHolder;
import org.apache.ignite.internal.configuration.tree.ConfigurationSource;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.apache.ignite.internal.configuration.util.ConfigurationUtil;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.platform.commons.support.AnnotationSupport;
import org.junit.platform.commons.support.HierarchyTraversalMode;

/**
 * JUnit extension to inject configuration instances into test classes.
 *
 * @see InjectConfiguration
 * @see InjectRevisionListenerHolder
 */
public class ConfigurationExtension implements BeforeEachCallback, AfterEachCallback,
        BeforeAllCallback, AfterAllCallback, ParameterResolver {
    /** JUnit namespace for the extension. */
    private static final Namespace NAMESPACE = Namespace.create(ConfigurationExtension.class);

    /** Key to store {@link ConfigurationAsmGenerator} in {@link ExtensionContext.Store}. */
    private static final Object CGEN_KEY = new Object();

    /** Key to store {@link ExecutorService} in {@link ExtensionContext.Store}. */
    private static final Object POOL_KEY = new Object();

    /** Key to store {@link StorageRevisionListenerHolderImpl} in {@link ExtensionContext.Store}. */
    private static final Object REVISION_LISTENER_HOLDER_KEY = new Object();

    /** {@inheritDoc} */
    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        context.getStore(NAMESPACE).put(POOL_KEY, newSingleThreadExecutor());
    }

    /** {@inheritDoc} */
    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        ExecutorService pool = context.getStore(NAMESPACE).remove(POOL_KEY, ExecutorService.class);

        pool.shutdownNow();
    }

    /** {@inheritDoc} */
    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        ConfigurationAsmGenerator cgen = new ConfigurationAsmGenerator();

        context.getStore(NAMESPACE).put(CGEN_KEY, cgen);

        Object testInstance = context.getRequiredTestInstance();

        ExecutorService pool = context.getStore(NAMESPACE).get(POOL_KEY, ExecutorService.class);

        StorageRevisionListenerHolderImpl revisionListenerHolder = new StorageRevisionListenerHolderImpl();

        context.getStore(NAMESPACE).put(REVISION_LISTENER_HOLDER_KEY, revisionListenerHolder);

        for (Field field : getInjectConfigurationFields(testInstance.getClass())) {
            field.setAccessible(true);

            InjectConfiguration annotation = field.getAnnotation(InjectConfiguration.class);

            field.set(testInstance, cfgValue(field.getType(), annotation, cgen, pool, revisionListenerHolder));
        }

        for (Field field : getInjectRevisionListenerHolderFields(testInstance.getClass())) {
            field.setAccessible(true);

            field.set(testInstance, revisionListenerHolder);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        context.getStore(NAMESPACE).remove(CGEN_KEY);
        context.getStore(NAMESPACE).remove(REVISION_LISTENER_HOLDER_KEY);
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsParameter(
            ParameterContext parameterContext,
            ExtensionContext extensionContext
    ) throws ParameterResolutionException {
        Class<?> parameterType = parameterContext.getParameter().getType();

        return (parameterContext.isAnnotated(InjectConfiguration.class) && supportsAsConfigurationType(parameterType))
                || (parameterContext.isAnnotated(InjectRevisionListenerHolder.class) && isRevisionListenerHolder(parameterType));
    }

    /** {@inheritDoc} */
    @Override
    public Object resolveParameter(
            ParameterContext parameterContext,
            ExtensionContext extensionContext
    ) throws ParameterResolutionException {
        if (parameterContext.isAnnotated(InjectConfiguration.class)) {
            Parameter parameter = parameterContext.getParameter();

            ConfigurationAsmGenerator cgen =
                    extensionContext.getStore(NAMESPACE).get(CGEN_KEY, ConfigurationAsmGenerator.class);

            StorageRevisionListenerHolderImpl revisionListenerHolder =
                    extensionContext.getStore(NAMESPACE).get(REVISION_LISTENER_HOLDER_KEY, StorageRevisionListenerHolderImpl.class);

            try {
                ExecutorService pool = extensionContext.getStore(NAMESPACE).get(POOL_KEY, ExecutorService.class);

                return cfgValue(parameter.getType(), parameter.getAnnotation(InjectConfiguration.class), cgen, pool,
                        revisionListenerHolder);
            } catch (ClassNotFoundException classNotFoundException) {
                throw new ParameterResolutionException(
                        "Cannot find a configuration schema class that matches " + parameter.getType().getCanonicalName(),
                        classNotFoundException
                );
            }
        } else if (parameterContext.isAnnotated(InjectRevisionListenerHolder.class)) {
            return extensionContext.getStore(NAMESPACE).get(REVISION_LISTENER_HOLDER_KEY, StorageRevisionListenerHolderImpl.class);
        } else {
            throw new ParameterResolutionException("Unknown parametr:" + parameterContext.getParameter());
        }
    }

    /**
     * Instantiates a configuration instance for injection.
     *
     * @param type Type of the field or parameter. Class name must end with {@code Configuration}.
     * @param annotation Annotation present on the field or parameter.
     * @param cgen Runtime code generator associated with the extension instance.
     * @param pool Single-threaded executor service to perform configuration changes.
     * @param revisionListenerHolder Configuration storage revision change listener holder.
     * @return Mock configuration instance.
     * @throws ClassNotFoundException If corresponding configuration schema class is not found.
     */
    private static Object cfgValue(
            Class<?> type,
            InjectConfiguration annotation,
            ConfigurationAsmGenerator cgen,
            ExecutorService pool,
            StorageRevisionListenerHolderImpl revisionListenerHolder
    ) throws ClassNotFoundException {
        // Trying to find a schema class using configuration naming convention. This code won't work for inner Java
        // classes, extension is designed to mock actual configurations from public API to configure Ignite components.
        Class<?> schemaClass = Class.forName(type.getCanonicalName() + "Schema");

        cgen.compileRootSchema(
                schemaClass,
                internalSchemaExtensions(List.of(annotation.internalExtensions())),
                polymorphicSchemaExtensions(List.of(annotation.polymorphicExtensions()))
        );

        // RootKey must be mocked, there's no way to instantiate it using a public constructor.
        RootKey rootKey = mock(RootKey.class);

        when(rootKey.key()).thenReturn("mock");
        when(rootKey.type()).thenReturn(LOCAL);
        when(rootKey.schemaClass()).thenReturn(schemaClass);
        when(rootKey.internal()).thenReturn(false);

        SuperRoot superRoot = new SuperRoot(s -> new RootInnerNode(rootKey, cgen.instantiateNode(schemaClass)));

        ConfigObject hoconCfg = ConfigFactory.parseString(annotation.value()).root();

        HoconConverter.hoconSource(hoconCfg).descend(superRoot);

        ConfigurationUtil.addDefaults(superRoot);

        if (!annotation.name().isEmpty()) {
            InnerNode root = superRoot.getRoot(rootKey);

            root.internalId(UUID.randomUUID());
            root.setInjectedNameFieldValue(annotation.name());
        }

        // Reference to the super root is required to make DynamicConfigurationChanger#change method atomic.
        var superRootRef = new AtomicReference<>(superRoot);

        // Reference that's required for notificator.
        var cfgRef = new AtomicReference<DynamicConfiguration<?, ?>>();

        cfgRef.set(cgen.instantiateCfg(rootKey, new DynamicConfigurationChanger() {
            /** {@inheritDoc} */
            @Override
            public CompletableFuture<Void> change(ConfigurationSource change) {
                return CompletableFuture.supplyAsync(() -> {
                    SuperRoot sr = superRootRef.get();

                    SuperRoot copy = sr.copy();

                    change.descend(copy);

                    ConfigurationUtil.dropNulls(copy);

                    if (superRootRef.compareAndSet(sr, copy)) {
                        long storageRevision = revisionListenerHolder.storageRev.incrementAndGet();
                        long notificationNumber = revisionListenerHolder.notificationListenerCnt.incrementAndGet();

                        List<CompletableFuture<?>> futures = new ArrayList<>();

                        futures.addAll(notifyListeners(
                                sr.getRoot(rootKey),
                                copy.getRoot(rootKey),
                                (DynamicConfiguration<InnerNode, ?>) cfgRef.get(),
                                storageRevision,
                                notificationNumber
                        ));

                        futures.addAll(revisionListenerHolder.notifyStorageRevisionListeners(storageRevision, notificationNumber));

                        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new));
                    }

                    return change(change);
                }, pool).thenCompose(Function.identity());
            }

            /** {@inheritDoc} */
            @Override
            public InnerNode getRootNode(RootKey<?, ?> rk) {
                return superRootRef.get().getRoot(rk);
            }

            /** {@inheritDoc} */
            @Override
            public <T> T getLatest(List<KeyPathNode> path) {
                return findEx(path, superRootRef.get());
            }

            /** {@inheritDoc} */
            @Override
            public long notificationCount() {
                return revisionListenerHolder.notificationListenerCnt.get();
            }
        }));

        touch(cfgRef.get());

        return cfgRef.get();
    }

    private static List<Field> getInjectConfigurationFields(Class<?> testClass) {
        return AnnotationSupport.findAnnotatedFields(
                testClass,
                InjectConfiguration.class,
                field -> supportsAsConfigurationType(field.getType()),
                HierarchyTraversalMode.TOP_DOWN
        );
    }

    private static List<Field> getInjectRevisionListenerHolderFields(Class<?> testClass) {
        return AnnotationSupport.findAnnotatedFields(
                testClass,
                InjectRevisionListenerHolder.class,
                field -> isRevisionListenerHolder(field.getType()),
                HierarchyTraversalMode.TOP_DOWN
        );
    }

    private static boolean supportsAsConfigurationType(Class<?> type) {
        return type.getCanonicalName().endsWith("Configuration");
    }

    private static boolean isRevisionListenerHolder(Class<?> type) {
        return ConfigurationStorageRevisionListenerHolder.class.isAssignableFrom(type);
    }

    /**
     * Implementation for {@link ConfigurationExtension}.
     */
    private static class StorageRevisionListenerHolderImpl implements ConfigurationStorageRevisionListenerHolder {
        final AtomicLong storageRev = new AtomicLong();

        final AtomicLong notificationListenerCnt = new AtomicLong();

        final ConfigurationListenerHolder<ConfigurationStorageRevisionListener> listeners = new ConfigurationListenerHolder<>();

        /** {@inheritDoc} */
        @Override
        public void listenUpdateStorageRevision(ConfigurationStorageRevisionListener listener) {
            listeners.addListener(listener, notificationListenerCnt.get());
        }

        /** {@inheritDoc} */
        @Override
        public void stopListenUpdateStorageRevision(ConfigurationStorageRevisionListener listener) {
            listeners.removeListener(listener);
        }

        private Collection<CompletableFuture<?>> notifyStorageRevisionListeners(long storageRevision, long notificationNumber) {
            List<CompletableFuture<?>> futures = new ArrayList<>();

            for (Iterator<ConfigurationStorageRevisionListener> it = listeners.listeners(notificationNumber); it.hasNext(); ) {
                ConfigurationStorageRevisionListener listener = it.next();

                try {
                    CompletableFuture<?> future = listener.onUpdate(storageRevision);

                    assert future != null;

                    if (future.isCompletedExceptionally() || future.isCancelled() || !future.isDone()) {
                        futures.add(future);
                    }
                } catch (Throwable t) {
                    futures.add(CompletableFuture.failedFuture(t));
                }
            }

            return futures;
        }
    }
}
