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

package org.apache.ignite.migrationtools.persistence.marshallers;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.util.ArrayList;
import java.util.List;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.dynamic.scaffold.subclass.ConstructorStrategy.Default;
import net.bytebuddy.implementation.MethodCall;
import net.bytebuddy.implementation.SuperMethodCall;

/**
 * {@link ObjectInputStream} implementation which prevents {@link ClassNotFoundException}s from custom client libraries.
 */
public final class ForeignObjectInputStream extends ObjectInputStream {
    private final List<Class<?>> dummyClasses;

    private final ClassLoader publicClassloader;

    /**
     * Constructor.
     *
     * @param in Base input stream.
     * @param classLoader classloader.
     * @throws IOException may be thrown.
     */
    public ForeignObjectInputStream(InputStream in, ClassLoader classLoader) throws IOException {
        super(in);
        this.enableResolveObject(true);
        this.dummyClasses = new ArrayList<>();
        this.publicClassloader = classLoader;
    }

    @Override
    protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
        Class<?> c;
        try {
            c = super.resolveClass(desc);
        } catch (ClassNotFoundException ex) {
            String className = desc.getName();
            if (!className.startsWith("org.apache.ignite.")) {
                throw ex;
            }

            c = new ByteBuddy()
                    .subclass(Object.class, Default.NO_CONSTRUCTORS)
                    .name(className)
                    .defineConstructor(Visibility.PRIVATE)
                    .intercept(SuperMethodCall.INSTANCE.andThen(MethodCall.run(() -> {
                        throw new RuntimeException(String.format("Cannot instantiate dummy object for class '%s'."
                                + " This class could not be found and a placeholder was generated.", className));
                    })))
                    .make()
                    .load(publicClassloader, ClassLoadingStrategy.Default.WRAPPER)
                    .getLoaded();

            this.dummyClasses.add(c);
        }

        return c;
    }

    @Override
    protected Object resolveObject(Object obj) {
        for (Class<?> klass : this.dummyClasses) {
            if (klass.isInstance(obj)) {
                // Found instance of a dummy object. This should not happen.
                throw new IllegalStateException("Found instance of dummy object: " + klass.getName());
            }

            // We could also replace the dummy object with null.
        }

        return obj;
    }
}
