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
import static org.apache.ignite.configuration.annotation.ConfigurationType.DISTRIBUTED;
import static org.apache.ignite.configuration.annotation.ConfigurationType.LOCAL;
import static org.apache.ignite.internal.configuration.notifications.ConfigurationNotifier.notifyListeners;
import static org.apache.ignite.internal.configuration.testframework.InjectConfiguration.MOCK_ROOT_NAME;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.findEx;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.polymorphicSchemaExtensions;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.schemaExtensions;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.touch;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigObject;
import java.lang.reflect.Field;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.ignite.configuration.ConfigurationModule;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.SuperRootChange;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.configuration.validation.ConfigurationValidationException;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.configuration.validation.Validator;
import org.apache.ignite.internal.configuration.CompoundModule;
import org.apache.ignite.internal.configuration.ConfigurationTreeGenerator;
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
import org.apache.ignite.internal.configuration.validation.ConfigurationValidator;
import org.apache.ignite.internal.configuration.validation.ConfigurationValidatorImpl;
import org.jetbrains.annotations.Nullable;
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

    private static final ConfigurationModule LOCAL_MODULE;

    private static final ConfigurationModule DISTRIBUTED_MODULE;

    static {
        // Automatically find all @InternalConfiguration and @PolymorphicConfigInstance classes
        // to avoid configuring extensions manually in every test.
        ServiceLoader<ConfigurationModule> modules = ServiceLoader.load(ConfigurationModule.class);

        List<ConfigurationModule> localModules = new ArrayList<>();
        List<ConfigurationModule> distributedModules = new ArrayList<>();

        modules.forEach(configurationModule -> {
            if (configurationModule.type() == LOCAL) {
                localModules.add(configurationModule);
            } else {
                distributedModules.add(configurationModule);
            }
        });

        LOCAL_MODULE = CompoundModule.local(localModules);
        DISTRIBUTED_MODULE = CompoundModule.distributed(distributedModules);
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

        Collection<Class<?>> extensions = configurationModule(annotation.type()).schemaExtensions();

        if (annotation.extensions().length > 0) {
            extensions = new ArrayList<>(extensions);
            extensions.addAll(List.of(annotation.extensions()));
        }

        Collection<Class<?>> polymorphicExtensions = configurationModule(annotation.type()).polymorphicSchemaExtensions();

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
        RootKey<?, ?, ?> rootKey = new RootKey<>(rootName(annotation), annotation.type(), schemaClass, false);

        SuperRoot superRoot = createSuperRoot(rootKey, schemaClass, cgen);

        // Use empty object as default.
        String hoconStr = annotation.value().isBlank() ? rootKey.key() + " : {}" : annotation.value();

        ConfigObject hoconCfg = ConfigFactory.parseString(hoconStr).root();

        HoconConverter.hoconSource(hoconCfg).descend(superRoot);

        if (!annotation.rootName().isBlank()) {
            patchWithDynamicDefault(annotation.type(), superRoot);
        }

        ConfigurationUtil.addDefaults(superRoot);

        if (!annotation.name().isBlank()) {
            InnerNode root = superRoot.getRoot(rootKey);

            root.internalId(UUID.randomUUID());
            root.setInjectedNameFieldValue(annotation.name());
        }

        superRoot.makeImmutable();

        ConfigurationValidator validator = configurationValidator(annotation, rootKey, schemaClass, cgen);

        validateConfiguration(validator, superRoot);

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

                    validateConfiguration(validator, sr, copy);

                    if (superRootRef.compareAndSet(sr, copy)) {
                        long storageRevision = storageRevisionCounter.incrementAndGet();
                        long notificationNumber = notificationListenerCounter.incrementAndGet();

                        Collection<CompletableFuture<?>> futures = notifyListeners(
                                sr.getRoot(rootKey),
                                copy.getRoot(rootKey),
                                (DynamicConfiguration<InnerNode, ?>) cfgRef.get(),
                                storageRevision,
                                notificationNumber
                        );

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

        patchIfRootExists(configurationModule(type), rootChange);
    }

    /**
     * Patches configuration with dynamic defaults if the required root exists.
     * Modules may try to access roots that don't exist in a test SuperRoot (when using custom rootName),
     * in which case the patching is silently skipped.
     */
    private static void patchIfRootExists(ConfigurationModule module, SuperRootChange rootChange) {
        try {
            module.patchConfigurationWithDynamicDefaults(rootChange);
        } catch (NoSuchElementException ignored) {
            // Module tried to access a root that doesn't exist in this SuperRoot - skip
        }
    }

    private static void validateConfiguration(@Nullable ConfigurationValidator validator, SuperRoot configuration) {
        if (validator == null) {
            return;
        }

        List<ValidationIssue> validationIssues = validator.validate(configuration);

        if (!validationIssues.isEmpty()) {
            throw new ConfigurationValidationException(validationIssues);
        }
    }

    private static void validateConfiguration(@Nullable ConfigurationValidator validator, SuperRoot curRoots, SuperRoot changes) {
        if (validator == null) {
            return;
        }

        List<ValidationIssue> validationIssues = validator.validate(curRoots, changes);

        if (!validationIssues.isEmpty()) {
            throw new ConfigurationValidationException(validationIssues);
        }
    }

    @Nullable
    private static ConfigurationValidator configurationValidator(
            InjectConfiguration annotation,
            RootKey<?, ?, ?> rootKey,
            Class<?> schemaClass,
            ConfigurationAsmGenerator cgen
    ) {
        if (!annotation.validate()) {
            return null;
        }

        ConfigurationTreeGenerator mockGenerator = mockConfigurationTreeGenerator(rootKey, schemaClass, cgen);

        Set<Validator<?, ?>> validators = configurationModule(annotation.type()).validators();

        return ConfigurationValidatorImpl.withDefaultValidators(mockGenerator, validators);
    }

    private static SuperRoot createSuperRoot(RootKey<?, ?, ?> rootKey, Class<?> schemaClass, ConfigurationAsmGenerator cgen) {
        // Accept both "mock" (HOCON convention per @InjectConfiguration docs) and rootKey.key()
        // (for patchConfigurationWithDynamicDefaults which uses the real root key).
        return new SuperRoot(s ->
                s.equals(rootKey.key()) || s.equals(MOCK_ROOT_NAME)
                        ? new RootInnerNode(rootKey, cgen.instantiateNode(schemaClass))
                        : null
        );
    }

    private static ConfigurationTreeGenerator mockConfigurationTreeGenerator(
            RootKey<?, ?, ?> rootKey, Class<?> schemaClass, ConfigurationAsmGenerator cgen
    ) {
        ConfigurationTreeGenerator generator = mock(ConfigurationTreeGenerator.class);

        when(generator.createSuperRoot()).thenAnswer(invocation -> createSuperRoot(rootKey, schemaClass, cgen));

        return generator;
    }

    private static ConfigurationModule configurationModule(ConfigurationType type) {
        return type == LOCAL ? LOCAL_MODULE : DISTRIBUTED_MODULE;
    }

    private static String rootName(InjectConfiguration annotation) {
        return annotation.rootName().isBlank() ? MOCK_ROOT_NAME : annotation.rootName();
    }
}
