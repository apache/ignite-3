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
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.apache.ignite.configuration.annotation.ConfigurationType.LOCAL;
import static org.apache.ignite.internal.configuration.notifications.ConfigurationNotifier.notifyListeners;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.findEx;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.polymorphicSchemaExtensions;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.schemaExtensions;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.touch;

import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigObject;
import java.lang.reflect.Field;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
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
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.configuration.annotation.PolymorphicConfigInstance;
import org.apache.ignite.internal.configuration.DynamicConfiguration;
import org.apache.ignite.internal.configuration.DynamicConfigurationChanger;
import org.apache.ignite.internal.configuration.RootInnerNode;
import org.apache.ignite.internal.configuration.SuperRoot;
import org.apache.ignite.internal.configuration.SuperRootChangeImpl;
import org.apache.ignite.internal.configuration.asm.ConfigurationAsmGenerator;
import org.apache.ignite.internal.configuration.direct.KeyPathNode;
import org.apache.ignite.internal.configuration.hocon.HoconConverter;
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
 */
public class ConfigurationExtension implements BeforeEachCallback, AfterEachCallback,
        BeforeAllCallback, AfterAllCallback, ParameterResolver {
    /** JUnit namespace for the extension. */
    private static final Namespace NAMESPACE = Namespace.create(ConfigurationExtension.class);

    /** Key to store {@link ConfigurationAsmGenerator} in {@link ExtensionContext.Store}. */
    private static final Object CGEN_KEY = new Object();

    /** Key to store {@link ExecutorService} in {@link ExtensionContext.Store}. */
    private static final Object POOL_KEY = new Object();

    private static final List<ConfigurationModule> LOCAL_MODULES = new ArrayList<>();
    private static final List<ConfigurationModule> DISTRIBUTED_MODULES = new ArrayList<>();

    /** All {@link ConfigurationExtension} classes in classpath. */
    private static final List<Class<?>> EXTENSIONS;

    /** All {@link PolymorphicConfigInstance} classes in classpath. */
    private static final List<Class<?>> POLYMORPHIC_EXTENSIONS;

    static {
        // Automatically find all @InternalConfiguration and @PolymorphicConfigInstance classes
        // to avoid configuring extensions manually in every test.
        ServiceLoader<ConfigurationModule> modules = ServiceLoader.load(ConfigurationModule.class);

        List<Class<?>> extensions = new ArrayList<>();
        List<Class<?>> polymorphicExtensions = new ArrayList<>();

        modules.forEach(configurationModule -> {
            extensions.addAll(configurationModule.schemaExtensions());
            polymorphicExtensions.addAll(configurationModule.polymorphicSchemaExtensions());
            if (configurationModule.type() == LOCAL) {
                LOCAL_MODULES.add(configurationModule);
            } else {
                DISTRIBUTED_MODULES.add(configurationModule);
            }
        });

        EXTENSIONS = List.copyOf(extensions);
        POLYMORPHIC_EXTENSIONS = List.copyOf(polymorphicExtensions);
    }

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        context.getStore(NAMESPACE).put(CGEN_KEY, new ConfigurationAsmGenerator());
        context.getStore(NAMESPACE).put(POOL_KEY, newSingleThreadExecutor());

        injectFields(context, true);
    }

    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        context.getStore(NAMESPACE).remove(CGEN_KEY);

        context.getStore(NAMESPACE).remove(POOL_KEY, ExecutorService.class).shutdownNow();
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        injectFields(context, false);
    }

    private static void injectFields(ExtensionContext context, boolean forStatic) throws Exception {
        Class<?> testClass = context.getRequiredTestClass();
        Object testInstance = context.getTestInstance().orElse(null);

        assert forStatic || testInstance != null;

        Store store = context.getStore(NAMESPACE);

        ConfigurationAsmGenerator cgen = store.get(CGEN_KEY, ConfigurationAsmGenerator.class);
        ExecutorService pool = store.get(POOL_KEY, ExecutorService.class);

        for (Field field : getInjectConfigurationFields(testClass, forStatic)) {
            field.setAccessible(true);

            InjectConfiguration annotation = field.getAnnotation(InjectConfiguration.class);

            Object cfgValue = cfgValue(field.getType(), annotation, cgen, pool);

            field.set(forStatic ? null : testInstance, cfgValue);
        }
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
    }

    @Override
    public boolean supportsParameter(
            ParameterContext parameterContext,
            ExtensionContext extensionContext
    ) throws ParameterResolutionException {
        Class<?> parameterType = parameterContext.getParameter().getType();

        return parameterContext.isAnnotated(InjectConfiguration.class) && supportsAsConfigurationType(parameterType);
    }

    @Override
    public Object resolveParameter(
            ParameterContext parameterContext,
            ExtensionContext extensionContext
    ) throws ParameterResolutionException {
        Store store = extensionContext.getStore(NAMESPACE);

        if (parameterContext.isAnnotated(InjectConfiguration.class)) {
            Parameter parameter = parameterContext.getParameter();

            ConfigurationAsmGenerator cgen = store.get(CGEN_KEY, ConfigurationAsmGenerator.class);

            try {
                ExecutorService pool = store.get(POOL_KEY, ExecutorService.class);

                return cfgValue(parameter.getType(), parameter.getAnnotation(InjectConfiguration.class), cgen, pool);
            } catch (ClassNotFoundException classNotFoundException) {
                throw new ParameterResolutionException(
                        "Cannot find a configuration schema class that matches " + parameter.getType().getCanonicalName(),
                        classNotFoundException
                );
            }
        } else {
            throw new ParameterResolutionException("Unknown parameter:" + parameterContext.getParameter());
        }
    }

    /**
     * Instantiates a configuration instance for injection.
     *
     * @param type Type of the field or parameter. Class name must end with {@code Configuration}.
     * @param annotation Annotation present on the field or parameter.
     * @param cgen Runtime code generator associated with the extension instance.
     * @param pool Single-threaded executor service to perform configuration changes.
     * @return Mock configuration instance.
     * @throws ClassNotFoundException If corresponding configuration schema class is not found.
     */
    private static Object cfgValue(
            Class<?> type,
            InjectConfiguration annotation,
            ConfigurationAsmGenerator cgen,
            ExecutorService pool
    ) throws ClassNotFoundException {
        // Trying to find a schema class using configuration naming convention. This code won't work for inner Java
        // classes, extension is designed to mock actual configurations from public API to configure Ignite components.
        Class<?> schemaClass = Class.forName(type.getCanonicalName() + "Schema");

        List<Class<?>> extensions = EXTENSIONS;
        List<Class<?>> polymorphicExtensions = POLYMORPHIC_EXTENSIONS;

        if (annotation.extensions().length > 0) {
            extensions = new ArrayList<>(extensions);
            extensions.addAll(List.of(annotation.extensions()));
        }

        if (annotation.polymorphicExtensions().length > 0) {
            polymorphicExtensions = new ArrayList<>(polymorphicExtensions);
            polymorphicExtensions.addAll(List.of(annotation.polymorphicExtensions()));
        }

        cgen.compileRootSchema(
                schemaClass,
                schemaExtensions(extensions),
                polymorphicSchemaExtensions(polymorphicExtensions)
        );

        // RootKey must be mocked, there's no way to instantiate it using a public constructor.
        RootKey<?, ?, ?> rootKey = new RootKey<>(
                annotation.rootName().isBlank() ? "mock" : annotation.rootName(),
                LOCAL,
                schemaClass,
                false
        );

        SuperRoot superRoot = new SuperRoot(s ->
                s.equals(rootKey.key())
                        ? new RootInnerNode(rootKey, cgen.instantiateNode(schemaClass))
                        : null
        );

        ConfigObject hoconCfg = ConfigFactory.parseString(annotation.value()).root();

        HoconConverter.hoconSource(hoconCfg).descend(superRoot);

        if (!annotation.rootName().isBlank()) {
            patchWithDynamicDefault(annotation.type(), superRoot);
        }

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

        AtomicLong storageRevisionCounter = new AtomicLong();

        AtomicLong notificationListenerCounter = new AtomicLong();

        cfgRef.set(cgen.instantiateCfg(rootKey, new DynamicConfigurationChanger() {
            @Override
            public CompletableFuture<Void> change(ConfigurationSource change) {
                return CompletableFuture.supplyAsync(() -> {
                    SuperRoot sr = superRootRef.get();

                    SuperRoot copy = sr.copy();

                    change.descend(copy);

                    ConfigurationUtil.dropNulls(copy);

                    if (superRootRef.compareAndSet(sr, copy)) {
                        long storageRevision = storageRevisionCounter.incrementAndGet();
                        long notificationNumber = notificationListenerCounter.incrementAndGet();

                        List<CompletableFuture<?>> futures = new ArrayList<>();

                        futures.addAll(notifyListeners(
                                sr.getRoot(rootKey),
                                copy.getRoot(rootKey),
                                (DynamicConfiguration<InnerNode, ?>) cfgRef.get(),
                                storageRevision,
                                notificationNumber
                        ));

                        return allOf(futures.toArray(CompletableFuture[]::new));
                    }

                    return change(change);
                }, pool).thenCompose(Function.identity());
            }

            @Override
            public InnerNode getRootNode(RootKey<?, ?, ?> rk) {
                return superRootRef.get().getRoot(rk);
            }

            @Override
            public <T> T getLatest(List<KeyPathNode> path) {
                return findEx(path, superRootRef.get());
            }

            @Override
            public long notificationCount() {
                return notificationListenerCounter.get();
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

    private static boolean supportsAsConfigurationType(Class<?> type) {
        return type.getCanonicalName().endsWith("Configuration");
    }

    private static void patchWithDynamicDefault(ConfigurationType type, SuperRoot superRoot) {
        SuperRootChangeImpl rootChange = new SuperRootChangeImpl(superRoot);
        if (type == LOCAL) {
            LOCAL_MODULES.forEach(module -> module.patchConfigurationWithDynamicDefaults(rootChange));
        } else {
            DISTRIBUTED_MODULES.forEach(module -> module.patchConfigurationWithDynamicDefaults(rootChange));
        }
    }
}
