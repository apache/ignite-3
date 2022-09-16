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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.tools.FileObject;
import javax.tools.ForwardingJavaFileManager;
import javax.tools.JavaFileManager;
import javax.tools.JavaFileObject;
import javax.tools.JavaFileObject.Kind;
import javax.tools.SimpleJavaFileObject;
import javax.tools.StandardLocation;

/**
 * A file manager implementation that stores all output in memory.
 */
final class InMemoryJavaFileManager extends ForwardingJavaFileManager<JavaFileManager> {
    private final Map<URI, JavaFileObject> cache = new HashMap<>();

    InMemoryJavaFileManager(JavaFileManager fileManager) {
        super(fileManager);
    }

    static URI uriForFileObject(Location location, String packageName, String relativeName) {
        StringBuilder uri = new StringBuilder("mem:///").append(location.getName()).append('/');
        if (!packageName.isEmpty()) {
            uri.append(packageName.replace('.', '/')).append('/');
        }
        uri.append(relativeName);
        return URI.create(uri.toString());
    }

    static URI uriForJavaFileObject(Location location, String className, Kind kind) {
        return URI.create("mem:///" + location.getName() + '/' + className.replace('.', '/') + kind.extension);
    }

    @Override
    public String inferBinaryName(Location location, JavaFileObject file) {
        if (file instanceof InMemoryJavaFileObject) {
            String fileName = file.getName();
            // We are only interested in "org.apache..."
            return fileName.substring(fileName.indexOf("org"), fileName.lastIndexOf('.')).replace('/', '.');
        }

        return super.inferBinaryName(location, file);
    }

    @Override
    public FileObject getFileForOutput(Location location, String packageName, String relativeName, FileObject sibling) {
        return cache.computeIfAbsent(
            uriForFileObject(location, packageName, relativeName),
            uri -> new InMemoryJavaFileObject(uri, Kind.OTHER)
        );
    }

    @Override
    public JavaFileObject getJavaFileForOutput(Location location, String className, Kind kind, FileObject sibling) {
        return cache.computeIfAbsent(
            uriForJavaFileObject(location, className, kind),
            uri -> new InMemoryJavaFileObject(uri, kind)
        );
    }

    @Override
    public Iterable<JavaFileObject> list(Location location, String packageName, Set<Kind> kinds, boolean recurse) throws IOException {
        List<JavaFileObject> list = new ArrayList<>();

        super.list(location, packageName, kinds, recurse).forEach(list::add);

        if (location == StandardLocation.CLASS_OUTPUT || location == StandardLocation.CLASS_PATH
                || location == StandardLocation.SOURCE_OUTPUT) {
            list.addAll(cache.values());
        }

        return list;
    }

    Map<URI, JavaFileObject> getOutputFiles() {
        return cache;
    }

    private static final class InMemoryJavaFileObject extends SimpleJavaFileObject {
        private byte[] data;

        InMemoryJavaFileObject(URI uri, Kind kind) {
            super(uri, kind);
        }

        @Override
        public InputStream openInputStream() throws IOException {
            return new ByteArrayInputStream(data());
        }

        @Override
        public OutputStream openOutputStream() {
            return new ByteArrayOutputStream() {
                @Override
                public void close() throws IOException {
                    super.close();

                    data = toByteArray();
                }
            };
        }

        @Override
        public CharSequence getCharContent(boolean ignoreEncodingErrors) throws IOException {
            return new String(data(), StandardCharsets.UTF_8);
        }

        private byte[] data() throws IOException {
            if (data == null) {
                throw new NoSuchFileException(uri.toString());
            }

            return data;
        }
    }
}

