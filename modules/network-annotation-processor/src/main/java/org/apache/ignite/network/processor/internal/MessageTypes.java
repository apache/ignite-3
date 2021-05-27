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

package org.apache.ignite.network.processor.internal;

import javax.lang.model.element.TypeElement;
import com.squareup.javapoet.ClassName;
import org.apache.ignite.network.annotations.ModuleMessageTypes;

/**
 * Wrapper around an element annotated with {@link ModuleMessageTypes}.
 */
public class MessageTypes {
    /** Class name of the currently processed element. */
    private final ClassName className;

    /** Annotation class of the currently processed element. */
    private final ModuleMessageTypes annotation;

    /**
     * @param moduleMessageTypes Element annotated with {@link ModuleMessageTypes}.
     */
    public MessageTypes(TypeElement moduleMessageTypes) {
        className = ClassName.get(moduleMessageTypes);
        annotation = moduleMessageTypes.getAnnotation(ModuleMessageTypes.class);
    }

    /**
     * Returns the package name of the annotated element.
     */
    public String packageName() {
        return className.packageName();
    }

    /**
     * Returns the {@link ModuleMessageTypes#moduleName()} declared in the annotation.
     */
    public String moduleName() {
        return capitalize(annotation.moduleName());
    }

    /**
     * Returns the {@link ModuleMessageTypes#moduleType()} declared in the annotation.
     */
    public short moduleType() {
        return annotation.moduleType();
    }

    /**
     * Returns the class name of the message factory that should be generated for the current module.
     */
    public ClassName messageFactoryClassName() {
        return ClassName.get(packageName(), moduleName() + "Factory");
    }

    /**
     * Creates a copy of the given string with the first letter capitalized.
     */
    private static String capitalize(String str) {
        return Character.toUpperCase(str.charAt(0)) + str.substring(1);
    }
}
