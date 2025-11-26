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

package org.apache.ignite.internal.network.processor;

import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ArrayTypeName;
import com.squareup.javapoet.TypeName;
import java.util.List;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.annotations.PropertyName;
import org.jetbrains.annotations.Nullable;

/** Сlass that contains useful constants and methods when generating classes for {@link NetworkMessage}. */
public class MessageGeneratorUtils {
    /** {@link Nullable} spec. */
    public static final AnnotationSpec NULLABLE_ANNOTATION_SPEC = AnnotationSpec.builder(Nullable.class).build();

    /** Type name of the {@code byte[]}. */
    public static final TypeName BYTE_ARRAY_TYPE = ArrayTypeName.of(TypeName.BYTE);

    /** Type name of the {@code byte @Nullable []}. */
    public static final TypeName NULLABLE_BYTE_ARRAY_TYPE = ArrayTypeName.of(TypeName.BYTE).annotated(NULLABLE_ANNOTATION_SPEC);

    /** Returns {@link true} if the method return value is marked with {@link Nullable}. */
    public static boolean methodReturnsNullableValue(ExecutableElement el) {
        TypeMirror returnType = el.getReturnType();

        TypeKind kind = returnType.getKind();

        if (kind == TypeKind.ARRAY) {
            List<? extends AnnotationMirror> annotations = returnType.getAnnotationMirrors();

            for (AnnotationMirror annotation : annotations) {
                DeclaredType annotationType = annotation.getAnnotationType();

                if (Nullable.class.getName().equals(annotationType.toString())) {
                    return true;
                }
            }

            return false;
        }

        return el.getAnnotation(Nullable.class) != null;
    }

    /** Returns {@link true} if the method return primitive value. */
    public static boolean methodReturnsPrimitive(ExecutableElement el) {
        return el.getReturnType().getKind().isPrimitive();
    }

    /** Adds postfix "ByteArray". */
    public static String addByteArrayPostfix(String s) {
        return s + "ByteArray";
    }

    /** Returns the property name for the message interface getter. */
    public static String propertyName(ExecutableElement getter) {
        PropertyName propertyNameAnnotation = getter.getAnnotation(PropertyName.class);

        return propertyNameAnnotation == null ? getter.getSimpleName().toString() : propertyNameAnnotation.value();
    }
}
