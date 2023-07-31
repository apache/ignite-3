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

package org.apache.ignite.internal.configuration.testframework;

import static java.lang.reflect.Modifier.isStatic;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.apache.ignite.configuration.annotation.ConfigurationType.LOCAL;
import static org.apache.ignite.internal.configuration.notifications.ConfigurationNotifier.notifyListeners;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.findEx;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.internalSchemaExtensions;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.polymorphicSchemaExtensions;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.touch;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigObject;
import java.lang.reflect.Executable;
import java.lang.reflect.Field;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.ignite.configuration.ConfigurationModule;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.annotation.InternalConfiguration;
import org.apache.ignite.configuration.annotation.PolymorphicConfigInstance;
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
import org.junit.jupiter.api.extension.ExtensionContext.Store;
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

    /** Key to store {@link StorageRevisionListenerHolderImpl} in {@link ExtensionContext.Store} for all tests. */
    private static final Object REVISION_LISTENER_ALL_TEST_HOLDER_KEY = new Object();

    /** Key to store {@link StorageRevisionListenerHolderImpl} in {@link ExtensionContext.Store} for each test. */
    private static final Object REVISION_LISTENER_PER_TEST_HOLDER_KEY = new Object();

    /** All {@link InternalConfiguration} classes in classpath. */
    private static final List<Class<?>> INTERNAL_EXTENSIONS;

    /** All {@link PolymorphicConfigInstance} classes in classpath. */
    private static final List<Class<?>> POLYMORPHIC_EXTENSIONS;

    static {
        // Automatically find all @InternalConfiguration and @PolymorphicConfigInstance classes
        // to avoid configuring extensions manually in every test.
        ServiceLoader<ConfigurationModule> modules = ServiceLoader.load(ConfigurationModule.class);

        List<Class<?>> internalExtensions = new ArrayList<>();
        List<Class<?>> polymorphicExtensions = new ArrayList<>();

        modules.forEach(configurationModule -> {
            internalExtensions.addAll(configurationModule.internalSchemaExtensions());
            polymorphicExtensions.addAll(configurationModule.polymorphicSchemaExtensions());
        });

        INTERNAL_EXTENSIONS = List.copyOf(internalExtensions);
        POLYMORPHIC_EXTENSIONS = List.copyOf(polymorphicExtensions);
    }

    /** {@inheritDoc} */
    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        context.getStore(NAMESPACE).put(CGEN_KEY, new ConfigurationAsmGenerator());
        context.getStore(NAMESPACE).put(POOL_KEY, newSingleThreadExecutor());

        injectFields(context, true);
    }

    /** {@inheritDoc} */
    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        context.getStore(NAMESPACE).remove(CGEN_KEY);

        context.getStore(NAMESPACE).remove(POOL_KEY, ExecutorService.class).shutdownNow();

        context.getStore(NAMESPACE).remove(REVISION_LISTENER_ALL_TEST_HOLDER_KEY);
    }

    /** {@inheritDoc} */
    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        injectFields(context, false);
    }

    private void injectFields(ExtensionContext context, boolean forStatic) throws Exception {
        Class<?> testClass = context.getRequiredTestClass();
        Object testInstance = context.getTestInstance().orElse(null);

        assert forStatic || testInstance != null;

        Store store = context.getStore(NAMESPACE);

        ConfigurationAsmGenerator cgen = store.get(CGEN_KEY, ConfigurationAsmGenerator.class);
        ExecutorService pool = store.get(POOL_KEY, ExecutorService.class);

        StorageRevisionListenerHolderImpl revisionListenerHolder = new StorageRevisionListenerHolderImpl();

        if (forStatic) {
            store.put(REVISION_LISTENER_ALL_TEST_HOLDER_KEY, revisionListenerHolder);
        } else {
            store.put(REVISION_LISTENER_PER_TEST_HOLDER_KEY, revisionListenerHolder);
        }

        for (Field field : getInjectConfigurationFields(testClass, forStatic)) {
            field.setAccessible(true);

            InjectConfiguration annotation = field.getAnnotation(InjectConfiguration.class);

            Object cfgValue = cfgValue(field.getType(), annotation, cgen, pool, revisionListenerHolder);

            field.set(forStatic ? null : testInstance, cfgValue);
        }

        for (Field field : getInjectRevisionListenerHolderFields(testClass, forStatic)) {
            field.setAccessible(true);

            field.set(forStatic ? null : testInstance, revisionListenerHolder);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        context.getStore(NAMESPACE).remove(REVISION_LISTENER_PER_TEST_HOLDER_KEY);
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
        Store store = extensionContext.getStore(NAMESPACE);

        StorageRevisionListenerHolderImpl revisionListenerHolder;

        if (isStaticExecutable(parameterContext.getDeclaringExecutable())) {
            revisionListenerHolder = store.get(REVISION_LISTENER_ALL_TEST_HOLDER_KEY, StorageRevisionListenerHolderImpl.class);
        } else {
            revisionListenerHolder = store.get(REVISION_LISTENER_PER_TEST_HOLDER_KEY, StorageRevisionListenerHolderImpl.class);
        }

        if (parameterContext.isAnnotated(InjectConfiguration.class)) {
            Parameter parameter = parameterContext.getParameter();

            ConfigurationAsmGenerator cgen = store.get(CGEN_KEY, ConfigurationAsmGenerator.class);

            try {
                ExecutorService pool = store.get(POOL_KEY, ExecutorService.class);

                return cfgValue(parameter.getType(), parameter.getAnnotation(InjectConfiguration.class), cgen, pool,
                        revisionListenerHolder);
            } catch (ClassNotFoundException classNotFoundException) {
                throw new ParameterResolutionException(
                        "Cannot find a configuration schema class that matches " + parameter.getType().getCanonicalName(),
                        classNotFoundException
                );
            }
        } else if (parameterContext.isAnnotated(InjectRevisionListenerHolder.class)) {
            return revisionListenerHolder;
        } else {
            throw new ParameterResolutionException("Unknown parameter:" + parameterContext.getParameter());
        }
    }

    private static boolean isStaticExecutable(Executable executable) {
        return isStatic(executable.getModifiers());
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

        List<Class<?>> internalExtensions = INTERNAL_EXTENSIONS;
        List<Class<?>> polymorphicExtensions = POLYMORPHIC_EXTENSIONS;

        if (annotation.internalExtensions().length > 0) {
            internalExtensions = new ArrayList<>(internalExtensions);
            internalExtensions.addAll(List.of(annotation.internalExtensions()));
        }

        if (annotation.polymorphicExtensions().length > 0) {
            polymorphicExtensions = new ArrayList<>(polymorphicExtensions);
            polymorphicExtensions.addAll(List.of(annotation.polymorphicExtensions()));
        }

        cgen.compileRootSchema(
                schemaClass,
                internalSchemaExtensions(internalExtensions),
                polymorphicSchemaExtensions(polymorphicExtensions)
        );

        // RootKey must be mocked, there's no way to instantiate it using a public constructor.
        RootKey rootKey = mock(RootKey.class, withSettings().lenient());

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

        superRoot.makeImmutable();

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

    private static List<Field> getInjectConfigurationFields(Class<?> testClass, boolean forStatic) {
        return AnnotationSupport.findAnnotatedFields(
                testClass,
                InjectConfiguration.class,
                field -> supportsAsConfigurationType(field.getType()) && (isStatic(field.getModifiers()) == forStatic),
                HierarchyTraversalMode.TOP_DOWN
        );
    }

    private static List<Field> getInjectRevisionListenerHolderFields(Class<?> testClass, boolean forStatic) {
        return AnnotationSupport.findAnnotatedFields(
                testClass,
                InjectRevisionListenerHolder.class,
                field -> isRevisionListenerHolder(field.getType()) && (isStatic(field.getModifiers()) == forStatic),
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
