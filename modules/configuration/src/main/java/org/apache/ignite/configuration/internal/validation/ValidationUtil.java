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

package org.apache.ignite.configuration.internal.validation;

import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.internal.util.KeysTrackingConfigurationVisitor;
import org.apache.ignite.configuration.tree.InnerNode;
import org.apache.ignite.configuration.tree.TraversableTreeNode;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.configuration.validation.Validator;

import static java.util.Collections.emptySet;

/** */
public class ValidationUtil {
    /**
     * Validate configuration changes.
     *
     * @param oldRoots Old values.
     * @param newRoots New values.
     * @param otherRoots
     * @param memberAnnotations
     * @param validators
     * @return Validation results.
     */
    public static List<ValidationIssue> validate(
        Map<RootKey<?, ?>, InnerNode> oldRoots,
        Map<RootKey<?, ?>, InnerNode> newRoots,
        Function<RootKey<?, ?>, InnerNode> otherRoots,
        Map<MemberKey, Set<Annotation>> memberAnnotations,
        Map<Class<? extends Annotation>, Set<Validator<?, ?>>> validators
    ) {
        List<ValidationIssue> issues = new ArrayList<>();

        for (Map.Entry<RootKey<?, ?>, ? extends TraversableTreeNode> entry : newRoots.entrySet()) {
            RootKey<?, ?> rootKey = entry.getKey();
            TraversableTreeNode newRoot = entry.getValue();

            newRoot.accept(rootKey.key(), new KeysTrackingConfigurationVisitor<>() {
                /** Inner nodes, last one always belongs to "current" leaf. */
                private Deque<InnerNode> innerNodes = new ArrayDeque<>();

                /** {@inheritDoc} */
                @Override protected Object visitInnerNode0(String key, InnerNode node) {
                    assert node != null;

                    // There cannot be validation annotations on the root itself. Condition is correct.
                    if (!innerNodes.isEmpty()) {
                        String currentKey = currentKey();

                        // Last dot should be trimmed.
                        validate(innerNodes.peek(), key, node, currentKey.substring(0, currentKey.length() - 1));
                    }

                    innerNodes.push(node);

                    try {
                        return super.visitInnerNode0(key, node);
                    }
                    finally {
                        innerNodes.pop();
                    }
                }

                /** {@inheritDoc} */
                @Override protected Void visitLeafNode0(String key, Serializable val) {
                    if (val == null) {
                        String message = "'" + currentKey() + "' configuration value is not initialized.";

                        issues.add(new ValidationIssue(message));
                    }
                    else
                        validate(innerNodes.peek(), key, val, currentKey());

                    return null;
                }

                /**
                 * Perform validation on the node's subnode.
                 *
                 * @param lastInnerNode Inner node that contains validated field.
                 * @param fieldName Name of the field.
                 * @param val Value of the field.
                 * @param currentKey Fully qualified key for the field.
                 */
                private void validate(InnerNode lastInnerNode, String fieldName, Object val, String currentKey) {
                    MemberKey memberKey = new MemberKey(lastInnerNode.getClass(), fieldName);

                    Set<Annotation> fieldAnnotations = memberAnnotations.computeIfAbsent(memberKey, k -> {
                        try {
                            Field field = lastInnerNode.schemaType().getDeclaredField(fieldName);

                            return Arrays.stream(field.getDeclaredAnnotations()).collect(Collectors.toSet());
                        }
                        catch (NoSuchFieldException e) {
                            // Should be impossible.
                            return emptySet();
                        }
                    });

                    for (Annotation annotation : fieldAnnotations) {
                        for (Validator<?, ?> validator : validators.getOrDefault(annotation.annotationType(), emptySet())) {
                            // Making this a compile-time check would be too expensive to implement.
                            assert assertTypesCoherence(validator.getClass(), annotation.annotationType(), val)
                                : "Validator coherence is violated [" +
                                "class=" + lastInnerNode.getClass().getSimpleName() + ", " +
                                "field=" + fieldName + ", " +
                                "annotation=" + annotation.annotationType().getSimpleName() + ", " +
                                "validator=" + validator.getClass().getSimpleName() + ']';

                            ValidationContextImpl<Object> ctx = new ValidationContextImpl<>(
                                rootKey,
                                oldRoots,
                                newRoots,
                                otherRoots,
                                val,
                                currentKey,
                                currentPath(),
                                issues
                            );

                            ((Validator<Annotation, Object>)validator).validate(annotation, ctx);
                        }
                    }
                }
            });
        }

        return issues;
    }

    /** */
    private static boolean assertTypesCoherence(
        Class<?> validatorClass,
        Class<? extends Annotation> annotationType,
        Object val
    ) {
        // Find superclass that directly extends Validator.
        if (!Arrays.asList(validatorClass.getInterfaces()).contains(Validator.class))
            return assertTypesCoherence(validatorClass.getSuperclass(), annotationType, val);

        Type genericSuperClass = Arrays.stream(validatorClass.getGenericInterfaces())
            .filter(i -> i instanceof ParameterizedType && ((ParameterizedType)i).getRawType() == Validator.class)
            .findAny()
            .get();

        if (!(genericSuperClass instanceof ParameterizedType))
            return false;

        ParameterizedType parameterizedSuperClass = (ParameterizedType)genericSuperClass;

        Type[] actualTypeParameters = parameterizedSuperClass.getActualTypeArguments();

        if (actualTypeParameters.length != 2)
            return false;

        if (actualTypeParameters[0] != annotationType)
            return false;

        if (!(actualTypeParameters[1] instanceof Class))
            return false;

        return val == null || ((Class<?>)actualTypeParameters[1]).isInstance(val);
    }
}
