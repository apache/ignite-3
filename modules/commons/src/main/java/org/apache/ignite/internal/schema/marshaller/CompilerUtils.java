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

package org.apache.ignite.internal.schema.marshaller;

import com.squareup.javapoet.JavaFile;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.FileObject;
import javax.tools.ForwardingJavaFileManager;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileManager;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.StandardLocation;
import javax.tools.ToolProvider;
import org.jetbrains.annotations.Nullable;

/**
 * Compiler utility class.
 */
public final class CompilerUtils {
    /**
     * @return Creates classloader for dynamically loaded in-memory classes.
     */
    public static ClassLoader dynamicClassLoader() {
        return AccessController.doPrivileged(new PrivilegedAction<>() {
            @Override public ClassLoader run() {
                return new MemoryClassLoader(new ConcurrentHashMap<>(), ClassLoader.getSystemClassLoader());
            }
        });
    }

    /**
     * Compiles code and load compiled classes.
     *
     * @param javafile Java file representation.
     * @return Classloader with compiled classes.
     */
    public static ClassLoader compileCode(JavaFile javafile) {
        final JavaCompiler cmp = ToolProvider.getSystemJavaCompiler();

        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        ConcurrentHashMap<String, byte[]> classes = classLoader instanceof MemoryClassLoader ?
            ((MemoryClassLoader)classLoader).classBytes : new ConcurrentHashMap<>();

        try (final MemoryJavaFileManager fileManager = new MemoryJavaFileManager(cmp.getStandardFileManager(null, null, null), classes)) {
            DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<>();

            JavaCompiler.CompilationTask task = cmp.getTask(null, fileManager, diagnostics, null, null, Collections.singletonList(javafile.toJavaFileObject()));

            if (task.call())
                return classLoader instanceof MemoryClassLoader ? classLoader :
                    new MemoryClassLoader(classes, ClassLoader.getSystemClassLoader());

            // TODO: write to log.
            for (Diagnostic diagnostic : diagnostics.getDiagnostics()) {
                System.out.println(diagnostic.getCode());
                System.out.println(diagnostic.getKind());
                System.out.println(diagnostic.getPosition());
                System.out.println(diagnostic.getStartPosition());
                System.out.println(diagnostic.getEndPosition());
                System.out.println(diagnostic.getSource());
                System.out.println(diagnostic.getMessage(null));
            }

            throw new IllegalStateException("Failed to compile code:\n" + javafile.toString());
        }
        catch (IOException ex) {
            throw new IllegalStateException("Failed to compile code.", ex);
        }
    }

    /**
     * @param iterables Iterables.
     * @return Concated iterable.
     */
    private static <E> Iterable<E> concat(Iterable<? extends E>... iterables) {
        return new Iterable<>() {
            private final Iterator<Iterable<? extends E>> it = Arrays.asList(iterables).iterator();

            /** {@inheritDoc} */
            @Override public Iterator<E> iterator() {
                return new Iterator<>() {
                    private Iterator<? extends E> curIt = Collections.emptyIterator();

                    /** {@inheritDoc} */
                    @Override public boolean hasNext() {
                        if (curIt == null || !curIt.hasNext())
                            advance();

                        return curIt.hasNext();
                    }

                    /** Switches to next iterable. */
                    private void advance() {
                        while (it.hasNext() && !curIt.hasNext())
                            curIt = it.next().iterator();
                    }

                    /** {@inheritDoc} */
                    @Override public E next() {
                        if (!hasNext())
                            throw new NoSuchElementException();

                        return curIt.next();
                    }

                    /** {@inheritDoc} */
                    @Override public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    /**
     * In-memory java file manager.
     */
    private static class MemoryJavaFileManager extends ForwardingJavaFileManager<JavaFileManager> {
        /** Classes code. */
        private final Map<String, byte[]> classesBytes;

        /**
         * Constructor.
         *
         * @param fileManager Java file manager.
         * @param classesBytes Classes code.
         */
        public MemoryJavaFileManager(JavaFileManager fileManager, Map<String, byte[]> classesBytes) {
            super(fileManager);
            this.classesBytes = classesBytes;
        }

        /**
         * Java '.class' in-memory file.
         */
        private static class MemoryJavaFileObject extends SimpleJavaFileObject {
            /** Class code. */
            private final byte[] classBytes;

            /**
             * Constructor.
             *
             * @param className Class name.
             * @param classBytes Class code.
             */
            MemoryJavaFileObject(String className, byte[] classBytes) {
                super(URI.create(className + Kind.CLASS.extension), Kind.CLASS);
                this.classBytes = classBytes;
            }

            /** {@inheritDoc} */
            @Override public InputStream openInputStream() {
                return new ByteArrayInputStream(classBytes);
            }
        }

        /**
         * A file object that stores Java bytecode into the classBytes map.
         */
        private class JavaClassOutputFile extends SimpleJavaFileObject {
            /** Class name. */
            private final String classname;

            /**
             * Constructor.
             *
             * @param classname Class name.
             */
            JavaClassOutputFile(String classname) {
                super(URI.create(classname), Kind.CLASS);
                this.classname = classname;
            }

            /** {@inheritDoc} */
            @Override public OutputStream openOutputStream() {
                return new ByteArrayOutputStream() {
                    @Override public void close() throws IOException {
                        super.close();

                        classesBytes.put(classname, toByteArray());
                    }
                };
            }
        }

        /** {@inheritDoc} */
        @Override public Iterable<JavaFileObject> list(Location location, String packageName,
            Set<JavaFileObject.Kind> kinds,
            boolean recurse) throws IOException {
            final Iterable<JavaFileObject> it = super.list(location, packageName, kinds, recurse);

            if (location == StandardLocation.CLASS_PATH && !classesBytes.isEmpty()) {
                assert kinds.contains(JavaFileObject.Kind.CLASS);

                Iterable<JavaFileObject> localClasses = new Iterable<>() {
                    @Override public Iterator<JavaFileObject> iterator() {
                        return classesBytes.keySet().stream()
                            .filter(cn -> !recurse || cn.lastIndexOf('.') == packageName.length())
                            .filter(cn -> cn.startsWith(packageName))
                            .map(cn -> getJavaFileObjectByName(cn))
                            .filter(Objects::nonNull).iterator();
                    }
                };

                return concat(localClasses, it);
            }
            else
                return it;
        }

        /** {@inheritDoc} */
        @Override public String inferBinaryName(Location location, JavaFileObject jfo) {
            if (!(jfo instanceof MemoryJavaFileObject)) {
                String result = super.inferBinaryName(location, jfo);
                assert result != null;
                return result;
            }

            // A [Java]FileObject's "name" looks like this: "/orc/codehaus/commons/compiler/Foo.java".
            // A [Java]FileObject's "binary name" looks like "java.lang.annotation.Retention".

            String bn = jfo.getName();
            if (bn.startsWith("/"))
                bn = bn.substring(1);

            if (!bn.endsWith(jfo.getKind().extension)) {
                throw new AssertionError(
                    "Name \"" + jfo.getName() + "\" does not match kind \"" + jfo.getKind() + "\""
                );
            }
            bn = bn.substring(0, bn.length() - jfo.getKind().extension.length());

            bn = bn.replace('/', '.');

            return bn;
        }

        /** {@inheritDoc} */
        @Override public JavaFileObject getJavaFileForInput(Location location, String className,
            JavaFileObject.Kind kind) throws IOException {
            if (location == StandardLocation.CLASS_OUTPUT) {
                JavaFileObject javaFileObject = getJavaFileObjectByName(className);

                if (javaFileObject != null)
                    return javaFileObject;
            }

            return fileManager.getJavaFileForInput(location, className, kind);
        }

        /** {@inheritDoc} */
        @Nullable private JavaFileObject getJavaFileObjectByName(String className) {
            final byte[] bytes = classesBytes.get(className);

            if (bytes != null)
                return new MemoryJavaFileObject(className, bytes);

            return null;
        }

        /** {@inheritDoc} */
        @Override public JavaFileObject getJavaFileForOutput(Location location,
            String className,
            JavaFileObject.Kind kind,
            FileObject sibling) throws IOException {
            if (kind == JavaFileObject.Kind.CLASS)
                return new JavaClassOutputFile(className);
            else
                return super.getJavaFileForOutput(location, className, kind, sibling);
        }
    }

    /**
     * Classloader for runtime compiled classes.
     */
    private static final class MemoryClassLoader extends URLClassLoader {
        /** Empty array. */
        private static final URL[] EMPTY_URLS = new URL[0];

        /** Classes code. */
        private final ConcurrentHashMap<String, byte[]> classBytes;

        /**
         * Constructor.
         *
         * @param classBytes Classes code holder.
         * @param parent Parent classloader.
         */
        MemoryClassLoader(ConcurrentHashMap<String, byte[]> classBytes, ClassLoader parent) {
            super(EMPTY_URLS, parent);

            this.classBytes = classBytes;
        }

        /** {@inheritDoc} */
        @Override protected Class<?> findClass(String className) throws ClassNotFoundException {
            byte[] buf = classBytes.get(className); // clear the bytes in map -- we don't need it anymore

            if (buf != null)
                return defineClass(className, buf, 0, buf.length);
            else
                return super.findClass(className);
        }
    }

    /** Stub. */
    private CompilerUtils() {
    }
}
