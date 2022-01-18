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

package org.apache.ignite.internal.network.serialization;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.invoke.MethodType;
import java.util.Objects;
import org.jetbrains.annotations.Nullable;

/**
 * Encapsulates special serialization methods like writeReplace()/readResolve() and so on for convenient invocation.
 */
class SpecialSerializationMethodsImpl implements SpecialSerializationMethods {
    /** Invoker that can be used to invoke writeReplace() on the target class. */
    @Nullable
    private final MethodInvoker writeReplace;

    /** Invoker that can be used to invoke readResolve() on the target class. */
    @Nullable
    private final MethodInvoker readResolve;

    /** Invoker that can be used to invoke writeObject() on the target class. */
    @Nullable
    private final MethodInvoker writeObject;

    /** Invoker that can be used to invoke readObject() on the target class. */
    @Nullable
    private final MethodInvoker readObject;

    /**
     * Creates a new instance from the provided descriptor.
     *
     * @param descriptor class descriptor on which class to operate
     */
    public SpecialSerializationMethodsImpl(ClassDescriptor descriptor) {
        writeReplace = descriptor.hasWriteReplace() ? writeReplaceInvoker(descriptor) : null;
        readResolve = descriptor.hasReadResolve() ? readResolveInvoker(descriptor) : null;
        writeObject = descriptor.hasWriteObject() ? writeObjectInvoker(descriptor) : null;
        readObject = descriptor.hasReadObject() ? readObjectInvoker(descriptor) : null;
    }

    private static MethodInvoker writeReplaceInvoker(ClassDescriptor descriptor) {
        try {
            return new MethodInvoker(
                    "writeReplace",
                    descriptor.clazz(),
                    MethodType.methodType(Object.class),
                    MethodType.methodType(Object.class, Object.class)
            );
        } catch (ReflectiveOperationException e) {
            throw new ReflectionException("Cannot find writeReplace() in " + descriptor.clazz(), e);
        }
    }

    private static MethodInvoker readResolveInvoker(ClassDescriptor descriptor) {
        try {
            return new MethodInvoker(
                    "readResolve",
                    descriptor.clazz(),
                    MethodType.methodType(Object.class),
                    MethodType.methodType(Object.class, Object.class)
            );
        } catch (ReflectiveOperationException e) {
            throw new ReflectionException("Cannot find readResolve() in " + descriptor.clazz(), e);
        }
    }

    private static MethodInvoker writeObjectInvoker(ClassDescriptor descriptor) {
        try {
            return new MethodInvoker(
                    "writeObject",
                    descriptor.clazz(),
                    MethodType.methodType(void.class, ObjectOutputStream.class),
                    MethodType.methodType(void.class, Object.class, ObjectOutputStream.class),
                    ObjectOutputStream.class
            );
        } catch (ReflectiveOperationException e) {
            throw new ReflectionException("Cannot find writeObject() in " + descriptor.clazz(), e);
        }
    }

    private static MethodInvoker readObjectInvoker(ClassDescriptor descriptor) {
        try {
            return new MethodInvoker(
                    "readObject",
                    descriptor.clazz(),
                    MethodType.methodType(void.class, ObjectInputStream.class),
                    MethodType.methodType(void.class, Object.class, ObjectInputStream.class),
                    ObjectInputStream.class
            );
        } catch (ReflectiveOperationException e) {
            throw new ReflectionException("Cannot find readObject() in " + descriptor.clazz(), e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public Object writeReplace(Object object) throws SpecialMethodInvocationException {
        Objects.requireNonNull(writeReplace);

        if (writeReplace.hasMethodHandle()) {
            try {
                return writeReplace.methodHandle().invokeExact(object);
            } catch (Error e) {
                throw e;
            } catch (Throwable e) {
                throw new SpecialMethodInvocationException("writeReplace() invocation failed on " + object, e);
            }
        } else {
            return writeReplace.invokeWithMethod(object);
        }
    }

    /** {@inheritDoc} */
    @Override
    public Object readResolve(Object object) throws SpecialMethodInvocationException {
        Objects.requireNonNull(readResolve);

        if (readResolve.hasMethodHandle()) {
            try {
                return readResolve.methodHandle().invokeExact(object);
            } catch (Error e) {
                throw e;
            } catch (Throwable e) {
                throw new SpecialMethodInvocationException("readResolve() invocation failed on " + object, e);
            }
        } else {
            return readResolve.invokeWithMethod(object);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void writeObject(Object object, ObjectOutputStream stream) throws SpecialMethodInvocationException {
        Objects.requireNonNull(writeObject);

        if (writeObject.hasMethodHandle()) {
            try {
                writeObject.methodHandle().invokeExact(object, stream);
            } catch (Error e) {
                throw e;
            } catch (Throwable e) {
                throw new SpecialMethodInvocationException("writeObject() invocation failed on " + object, e);
            }
        } else {
            writeObject.invokeWithMethod(object, stream);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void readObject(Object object, ObjectInputStream stream) throws SpecialMethodInvocationException {
        Objects.requireNonNull(readObject);

        if (readObject.hasMethodHandle()) {
            try {
                readObject.methodHandle().invokeExact(object, stream);
            } catch (Error e) {
                throw e;
            } catch (Throwable e) {
                throw new SpecialMethodInvocationException("readObject() invocation failed on " + object, e);
            }
        } else {
            readObject.invokeWithMethod(object, stream);
        }
    }
}
