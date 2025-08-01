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

package org.apache.ignite.migrationtools.persistence;

import java.io.File;
import java.io.FileFilter;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.persistence.filename.PdsFolderResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility class to handle Ignite 2 persistence work dirs. */
public class Ignite2PersistenceTools {

    private static final Logger LOGGER = LoggerFactory.getLogger(Ignite2PersistenceTools.class);

    // UUIDs as they are masked by org.apache.ignite.internal.util.IgniteUtils#maskForFileName
    private static final Pattern MASKED_UUID_PATTERN =
            Pattern.compile("[a-fA-F0-9]{8}_[a-fA-F0-9]{4}_[a-fA-F0-9]{4}_[a-fA-F0-9]{4}_[a-fA-F0-9]{12}");

    private static final Method FIND_PERSISTENT_STORAGE_PATH_METHOD;

    private static final Method FIND_NODE_FOLDERS_METHOD;

    private static final FileFilter DB_SUBFOLDERS_OLD_STYLE_FILTER;

    static {
        try {
            FIND_PERSISTENT_STORAGE_PATH_METHOD =
                    PdsFolderResolver.class.getDeclaredMethod("resolvePersistentStoreBasePath", boolean.class);
            FIND_PERSISTENT_STORAGE_PATH_METHOD.setAccessible(true);
            FIND_NODE_FOLDERS_METHOD = PdsFolderResolver.class.getDeclaredMethod("getNodeIndexSortedCandidates", File.class);
            FIND_NODE_FOLDERS_METHOD.setAccessible(true);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    static {
        try {
            Field field = PdsFolderResolver.class.getDeclaredField("DB_SUBFOLDERS_OLD_STYLE_FILTER");
            field.setAccessible(true);
            DB_SUBFOLDERS_OLD_STYLE_FILTER = (FileFilter) field.get(null);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Compute a list of {@link NodeFolderDescriptor}s available for the provided Ignite 2 cluster configuration.
     *
     * @param cfg The Ignite 2 cluster configuration.
     * @return List of node folders.
     */
    public static List<NodeFolderDescriptor> nodeFolderCandidates(IgniteConfiguration cfg) {
        if (cfg.getWorkDirectory() == null) {
            throw new IllegalArgumentException("WorkDirectory must not be null");
        }

        if (cfg.getDataStorageConfiguration() == null) {
            throw new IllegalArgumentException("DataStorageConfiguration must not be null");
        }

        File dbFolderFile;
        List<PdsFolderResolver.FolderCandidate> candidates;

        try {
            var folderResolver = new PdsFolderResolver<>(cfg, null, null, null);
            dbFolderFile = (File) FIND_PERSISTENT_STORAGE_PATH_METHOD.invoke(folderResolver, false);
            candidates = (List<PdsFolderResolver.FolderCandidate>) FIND_NODE_FOLDERS_METHOD.invoke(folderResolver, dbFolderFile);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }

        // Fallback trying to find folders using the old naming convention. Adapted from PDSResolver
        if (candidates == null || candidates.isEmpty()) {
            final File[] oldStyleFolders = dbFolderFile.listFiles(DB_SUBFOLDERS_OLD_STYLE_FILTER);

            if (oldStyleFolders != null && oldStyleFolders.length != 0) {
                List<NodeFolderDescriptor> ret = new ArrayList<>(oldStyleFolders.length);

                for (File folder : oldStyleFolders) {
                    String folderName = folder.getName();
                    Matcher matcher = MASKED_UUID_PATTERN.matcher(folderName);
                    if (matcher.matches()) {
                        String uuidStr = folderName.replace('_', '-');
                        var uuid = UUID.fromString(uuidStr);
                        ret.add(new NodeFolderDescriptor(folder, uuid));
                    } else {
                        LOGGER.warn("Could not capture consistentId (UUID) from node folder, falling back to String: {}", folder.getPath());
                        ret.add(new NodeFolderDescriptor(folder, folderName));
                    }
                }

                return ret;
            } else {
                return Collections.emptyList();
            }
        } else {
            return candidates.stream()
                    .map(folderCandidate -> new NodeFolderDescriptor(folderCandidate.subFolderFile(), folderCandidate.uuid()))
                    .collect(Collectors.toList());
        }
    }

    /**
     * Simple descriptor of discovered node folders.
     */
    public static class NodeFolderDescriptor {
        /** Absolute file path pointing to DB subfolder within DB storage root folder. */
        private final File subFolderFile;

        /** ConsistentId of the Node. */
        private final Serializable consistentId;

        public NodeFolderDescriptor(File subFolderFile, Serializable consistentId) {
            this.subFolderFile = subFolderFile;
            this.consistentId = consistentId;
        }

        /**
         * consistentId.
         *
         * @return Uuid contained in file name, is to be set as consistent ID.
         */
        public Serializable consistentId() {
            return consistentId;
        }

        /**
         * subFolderFile.
         *
         * @return Absolute file path pointing to DB subfolder within DB storage root folder.
         */
        public File subFolderFile() {
            return subFolderFile;
        }
    }
}
