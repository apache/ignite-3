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

package org.apache.ignite.internal.catalog.storage.serialization.utils;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogEntrySerializerProvider;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectSerializer;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogSerializer;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntry;
import org.apache.ignite.internal.catalog.storage.serialization.VersionAwareSerializer;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.jetbrains.annotations.Nullable;

/**
 * Serializers registry builder.
 * TODO explain how it works.
 */
public class SerializerRegistryBuilder {
    private final @Nullable CatalogEntrySerializerProvider provider;
    private final List<String> classpathList;

    public SerializerRegistryBuilder(List<String> classpathList, @Nullable CatalogEntrySerializerProvider provider) {
        this.provider = provider;
        this.classpathList = classpathList;
    }

    /**
     * Returns a registry (map) of available serializers.
     *
     * @return Registry of available serializers.
     */
    public Int2ObjectMap<VersionAwareSerializer<? extends MarshallableEntry>[]> build() {
        try {
            Predicate<Class<?>> filter = clazz ->
                    CatalogObjectSerializer.class.isAssignableFrom(clazz) && clazz.isAnnotationPresent(CatalogSerializer.class);
            List<Class<?>> classes = scanClasspaths(classpathList, filter);
            Map<Integer, List<VersionAwareSerializer<? extends MarshallableEntry>>> mapByType = mapSerializersByType(classes);

            Int2ObjectMap<VersionAwareSerializer<? extends MarshallableEntry>[]> resultMap = new Int2ObjectOpenHashMap<>(mapByType.size());

            for (Map.Entry<Integer, List<VersionAwareSerializer<? extends MarshallableEntry>>> entry : mapByType.entrySet()) {
                List<VersionAwareSerializer<? extends MarshallableEntry>> serializers = entry.getValue();
                int typeId = entry.getKey();

                VersionAwareSerializer<? extends MarshallableEntry>[] orderedSerializers = new VersionAwareSerializer[serializers.size()];

                for (VersionAwareSerializer<? extends MarshallableEntry> serializer : serializers) {
                    int versionIdx = serializer.version() - 1;

                    if (versionIdx >= orderedSerializers.length) {
                        throw new IllegalArgumentException(IgniteStringFormatter.format(
                                "The serializer version must be incremented by one [type={}, ver={}, max={}, class={}].",
                                typeId, serializer.version(), orderedSerializers.length, serializer.delegate().getClass()));
                    }

                    if (orderedSerializers[versionIdx] != null) {
                        throw new IllegalArgumentException(IgniteStringFormatter.format(
                                "Duplicate serializer version [serializer1={}, serializer2={}].",
                                orderedSerializers[versionIdx].delegate().getClass(), serializer.delegate().getClass()));
                    }

                    orderedSerializers[versionIdx] = serializer;
                }

                resultMap.put(typeId, orderedSerializers);
            }

            return Int2ObjectMaps.unmodifiable(resultMap);
        } catch (IOException | ClassNotFoundException e) {
            throw wrapException("Classpath scanning failed.", e);
        }
    }

    private Map<Integer, List<VersionAwareSerializer<? extends MarshallableEntry>>> mapSerializersByType(Collection<Class<?>> classes)
            throws IOException, ClassNotFoundException {
        Map<Integer, List<VersionAwareSerializer<? extends MarshallableEntry>>> result = new HashMap<>();

        for (Class<?> clazz : classes) {
            CatalogSerializer ann = clazz.getAnnotation(CatalogSerializer.class);

            assert ann != null;

            List<VersionAwareSerializer<? extends MarshallableEntry>> serializers =
                    result.computeIfAbsent(ann.type().id(), v -> new ArrayList<>());

            try {
                CatalogObjectSerializer<? extends MarshallableEntry> serializer = instantiate(clazz);

                if (ann.version() <= 0) {
                    throw new IllegalArgumentException("Serializer version must be positive [class=" + clazz.getCanonicalName() + "].");
                }

                serializers.add(new VersionAwareSerializer<>(serializer, ann.version()));
            } catch (InvocationTargetException | InstantiationException | IllegalAccessException e) {
                throw wrapException("Cannot instantiate serializer [class=" + clazz + "].", e);
            }
        }

        return result;
    }

    private CatalogObjectSerializer<? extends MarshallableEntry> instantiate(Class<?> cls)
            throws InvocationTargetException, InstantiationException, IllegalAccessException {
        Constructor<?>[] constructors = cls.getDeclaredConstructors();

        for (Constructor<?> constructor : constructors) {
            constructor.setAccessible(true);

            if (constructor.getParameterCount() == 0) {
                return (CatalogObjectSerializer<? extends MarshallableEntry>) constructor.newInstance();
            }

            if (provider != null && constructor.getParameterCount() == 1 && CatalogEntrySerializerProvider.class.isAssignableFrom(
                    constructor.getParameterTypes()[0])) {
                return (CatalogObjectSerializer<? extends MarshallableEntry>) constructor.newInstance(provider);
            }
        }

        throw new IllegalStateException("Unable to create serializer, required constructor was not found [class=" + cls + "].");
    }

    static List<Class<?>> scanClasspaths(List<String> packageNames, Predicate<Class<?>> filter) throws ClassNotFoundException, IOException {
        List<Class<?>> result = new ArrayList<>();

        for (String packageName : packageNames) {
            result.addAll(scanClasspath(packageName, filter));
        }

        return result;
    }

    /**
     * Scans all classes accessible from the context class loader which belong to the given package and subpackages.
     *
     * @param packageName The base package.
     */
    static List<Class<?>> scanClasspath(String packageName, Predicate<Class<?>> filter) throws ClassNotFoundException, IOException {
        Objects.requireNonNull(filter, "filter");

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        assert classLoader != null;

        String path = packageName.replace('.', '/');

        Enumeration<URL> resources = classLoader.getResources(path);

        List<Class<?>> classes = new ArrayList<>();

        while (resources.hasMoreElements()) {
            URL resource = resources.nextElement();

            // TODO rework this shit.
            if ("jar".equals(resource.getProtocol())) {
                // build jar file name, then loop through zipped entries
                String jarFileName = URLDecoder.decode(resource.getFile(), StandardCharsets.UTF_8);
                jarFileName = jarFileName.substring(5, jarFileName.indexOf('!'));
                try (JarFile jf = new JarFile(jarFileName)) {
                    Enumeration<JarEntry> jarEntries = jf.entries();
                    while (jarEntries.hasMoreElements()) {
                        String entryName = jarEntries.nextElement().getName();
                        if (entryName.startsWith(path) && entryName.endsWith(".class")) {
                            entryName = entryName.substring(path.length() + 1, entryName.lastIndexOf('.'));

                            String clsName = packageName + "." + entryName.replace("/", ".");
                            Class<?> clz = Class.forName(clsName);

                            if (filter.test(clz)) {
                                classes.add(clz);
                            }
                        }
                    }
                }

                continue;
            }

            List<Class<?>> dirClasses = findClasses(new File(resource.getFile()), packageName, filter);

            classes.addAll(dirClasses);
        }

        return classes;
    }

    /**
     * Recursive method used to find all classes in a given directory and sub directories.
     *
     * @param dir The base directory
     * @param packageName The package name for classes found inside the base directory
     * @param filter Classes filter.
     * @return The classes.
     */
    private static List<Class<?>> findClasses(File dir, String packageName, Predicate<Class<?>> filter) throws ClassNotFoundException {
        List<Class<?>> classes = new ArrayList<>();

        if (!dir.exists()) {
            return classes;
        }

        File[] files = dir.listFiles();

        if (files == null) {
            return classes;
        }

        for (File file : files) {
            if (file.isDirectory()) {
                assert !file.getName().contains(".") : file.getName();

                classes.addAll(findClasses(file, packageName + "." + file.getName(), filter));
            } else if (file.getName().endsWith(".class")) {
                String className = packageName + '.' + file.getName().substring(0, file.getName().length() - 6);

                Class<?> clazz = Class.forName(className);

                if (filter.test(clazz)) {
                    classes.add(clazz);
                }
            }
        }
        return classes;
    }

    // TODO Remove this method.
    private static RuntimeException wrapException(String message, Exception ex) {
        return new RuntimeException(message, ex);
    }
}
