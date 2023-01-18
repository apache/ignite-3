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
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.appendKey;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.configuration.validation.Validator;
import org.apache.ignite.internal.configuration.SuperRoot;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.apache.ignite.internal.configuration.util.AnyNodeConfigurationVisitor;
import org.apache.ignite.internal.configuration.util.KeysTrackingConfigurationVisitor;
import org.apache.ignite.lang.IgniteInternalException;
import org.jetbrains.annotations.Nullable;

/**
 * Utility class for configuration validation.
 */
public class ValidationUtil {
    /**
     * Validate configuration changes.
     *
     * @param oldRoots Old known roots.
     * @param newRoots New roots.
     * @param otherRoots Provider for arbitrary roots that might not be associated with the same storage.
     * @param memberAnnotationsCache Mutable map that contains annotations associated with corresponding member keys.
     * @param validators Current validators map to look into.
     * @return List of validation results.
     */
    public static List<ValidationIssue> validate(
            SuperRoot oldRoots,
            SuperRoot newRoots,
            Function<RootKey<?, ?>, InnerNode> otherRoots,
            Map<MemberKey, Map<Annotation, Set<Validator<?, ?>>>> memberAnnotationsCache,
            List<? extends Validator<?, ?>> validators
    ) {
        List<ValidationIssue> issues = new ArrayList<>();

        newRoots.traverseChildren(new KeysTrackingConfigurationVisitor<>() {
            /** {@inheritDoc} */
            @Override
            protected Object doVisitInnerNode(String key, InnerNode innerNode) {
                assert innerNode != null;

                innerNode.traverseChildren(new AnyNodeConfigurationVisitor<Void>() {
                    @Override
                    protected Void visitNode(String key, Object node) {
                        validate(innerNode, key, node);

                        return null;
                    }
                }, true);

                return super.doVisitInnerNode(key, innerNode);
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

                Map<Annotation, Set<Validator<?, ?>>> fieldAnnotations = memberAnnotationsCache.computeIfAbsent(memberKey, k -> {
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
                                otherRoots,
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

    private static @Nullable Field findSchemaField(InnerNode innerNode, String schemaFieldName) throws NoSuchFieldException {
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
        } else if (innerNode.internalSchemaTypes() != null) {
            // Linear search to not fight with NoSuchFieldException.
            for (Class<?> internalSchemaType : innerNode.internalSchemaTypes()) {
                for (Field field : internalSchemaType.getDeclaredFields()) {
                    if (field.getName().equals(schemaFieldName)) {
                        return field;
                    }
                }
            }
        }

        return schemaType.getDeclaredField(schemaFieldName);
    }
}
