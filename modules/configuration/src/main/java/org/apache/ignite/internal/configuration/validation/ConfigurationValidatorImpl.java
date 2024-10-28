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

package org.apache.ignite.internal.configuration.validation;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.addDefaults;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.appendKey;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.dropNulls;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.configuration.validation.Validator;
import org.apache.ignite.internal.configuration.ConfigurationTreeGenerator;
import org.apache.ignite.internal.configuration.SuperRoot;
import org.apache.ignite.internal.configuration.hocon.HoconConverter;
import org.apache.ignite.internal.configuration.tree.ConfigurationSource;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.apache.ignite.internal.configuration.util.AnyNodeConfigurationVisitor;
import org.apache.ignite.internal.configuration.util.KeysTrackingConfigurationVisitor;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link ConfigurationValidator}.
 */
public class ConfigurationValidatorImpl implements ConfigurationValidator {

    /** Set of default configuration validators. */
    private static final Set<Validator<?, ?>> DEFAULT_VALIDATORS = Set.of(
            new ImmutableValidator(),
            new OneOfValidator(),
            new ExceptKeysValidator(),
            new PowerOfTwoValidator(),
            new RangeValidator(),
            new NotBlankValidator()
    );

    /** Lazy annotations cache for configuration schema fields. */
    private final Map<MemberKey, Map<Annotation, Set<Validator<?, ?>>>> cachedAnnotations = new ConcurrentHashMap<>();

    /** Runtime implementations generator for node classes. */
    private final ConfigurationTreeGenerator generator;

    /** Validators. */
    private final Set<? extends Validator<?, ?>> validators;

    /**
     * Constructor.
     *
     * @param validators Validators.
     */
    public ConfigurationValidatorImpl(
            ConfigurationTreeGenerator generator,
            Set<? extends Validator<?, ?>> validators
    ) {
        assert generator != null;
        assert validators != null;

        this.generator = generator;
        this.validators = validators;
    }

    /**
     * Create {@link ConfigurationValidatorImpl} with the default validators ({@link ConfigurationValidatorImpl#DEFAULT_VALIDATORS}) and the
     * provided ones.
     *
     * @param validators Validators.
     * @return Configuration validator.
     */
    public static ConfigurationValidatorImpl withDefaultValidators(ConfigurationTreeGenerator generator,
            Set<? extends Validator<?, ?>> validators) {
        HashSet<Validator<?, ?>> validators0 = new HashSet<>(DEFAULT_VALIDATORS);
        validators0.addAll(validators);
        return new ConfigurationValidatorImpl(generator, validators0);
    }

    /** {@inheritDoc} */
    @Override
    public List<ValidationIssue> validateHocon(String cfg) {
        try {
            Config config = ConfigFactory.parseString(cfg);
            return validate(HoconConverter.hoconSource(config.root()));
        } catch (ConfigException.Parse e) {
            throw new IllegalArgumentException(e);
        }
    }


    /** {@inheritDoc} */
    @Override
    public List<ValidationIssue> validate(ConfigurationSource src) {
        SuperRoot changes = emptySuperRoot();
        src.descend(changes);
        addDefaults(changes);
        dropNulls(changes);
        return validate(emptySuperRoot(), changes);
    }

    /** {@inheritDoc} */
    @Override
    public List<ValidationIssue> validate(SuperRoot newRoots) {
        return validate(emptySuperRoot(), newRoots);
    }

    /** {@inheritDoc} */
    @Override
    public List<ValidationIssue> validate(SuperRoot oldRoots, SuperRoot newRoots) {
        List<ValidationIssue> issues = new ArrayList<>();
        newRoots.traverseChildren(new KeysTrackingConfigurationVisitor<>() {
            /** {@inheritDoc} */
            @Override
            protected Object doVisitInnerNode(Field field, String key, InnerNode innerNode) {
                assert innerNode != null;

                innerNode.traverseChildren(new AnyNodeConfigurationVisitor<Void>() {
                    @Override
                    protected Void visitNode(String key, Object node) {
                        validate(innerNode, key, node);

                        return null;
                    }
                }, true);

                return super.doVisitInnerNode(field, key, innerNode);
            }

            /**
             * Perform validation on the node's subnode.
             *
             * @param lastInnerNode Inner node that contains validated field.
             * @param fieldName Name of the field.
             * @param val Value of the field.
             */
            private void validate(InnerNode lastInnerNode, String fieldName, Object val) {
                if (val == null) {
                    String message = "'" + (currentKey() + fieldName) + "' configuration value is not initialized.";

                    issues.add(new ValidationIssue(currentKey(), message));

                    return;
                }

                MemberKey memberKey = new MemberKey(lastInnerNode.schemaType(), fieldName);

                Map<Annotation, Set<Validator<?, ?>>> fieldAnnotations = cachedAnnotations.computeIfAbsent(memberKey, k -> {
                    try {
                        Field field = findSchemaField(lastInnerNode, fieldName);

                        assert field != null : memberKey;

                        return Stream.of(field.getDeclaredAnnotations()).collect(toMap(identity(), annotation ->
                                validators.stream()
                                        .filter(validator -> validator.canValidate(
                                                annotation.annotationType(),
                                                field.getType(),
                                                field.isAnnotationPresent(NamedConfigValue.class)
                                        ))
                                        .collect(Collectors.toSet()))
                        );
                    } catch (Exception e) {
                        // Should be impossible.
                        throw new IgniteInternalException(e);
                    }
                });

                if (fieldAnnotations.isEmpty()) {
                    return;
                }

                String currentKey = currentKey() + fieldName;
                List<String> currentPath = appendKey(currentPath(), fieldName);

                for (Entry<Annotation, Set<Validator<?, ?>>> entry : fieldAnnotations.entrySet()) {
                    Annotation annotation = entry.getKey();

                    for (Validator<?, ?> validator : entry.getValue()) {
                        ValidationContextImpl<Object> ctx = new ValidationContextImpl<>(
                                oldRoots,
                                newRoots,
                                val,
                                currentKey,
                                currentPath,
                                issues
                        );

                        ((Validator<Annotation, Object>) validator).validate(annotation, ctx);
                    }
                }
            }
        }, true);

        return issues;
    }

    private @Nullable Field findSchemaField(InnerNode innerNode, String schemaFieldName) throws NoSuchFieldException {
        Class<?> schemaType = innerNode.schemaType();

        if (innerNode.isPolymorphic() || innerNode.extendsAbstractConfiguration()) {
            // Linear search to not fight with NoSuchFieldException.
            for (Field field : schemaType.getDeclaredFields()) {
                if (field.getName().equals(schemaFieldName)) {
                    return field;
                }
            }

            // Get parent schema.
            schemaType = schemaType.getSuperclass();
        } else if (innerNode.extensionSchemaTypes() != null) {
            // Linear search to not fight with NoSuchFieldException.
            for (Class<?> extensionSchemaType : innerNode.extensionSchemaTypes()) {
                for (Field field : extensionSchemaType.getDeclaredFields()) {
                    if (field.getName().equals(schemaFieldName)) {
                        return field;
                    }
                }
            }
        }

        return schemaType.getDeclaredField(schemaFieldName);
    }

    private SuperRoot emptySuperRoot() {
        return generator.createSuperRoot();
    }
}
