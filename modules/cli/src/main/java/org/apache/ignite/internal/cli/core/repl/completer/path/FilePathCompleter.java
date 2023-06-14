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

package org.apache.ignite.internal.cli.core.repl.completer.path;

import static org.apache.ignite.internal.cli.util.ArrayUtils.findLastNotEmptyWord;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.cli.core.repl.completer.DynamicCompleter;

/**
 * Scans file system and completes typed words.
 */
public class FilePathCompleter implements DynamicCompleter {

    @Override
    public List<String> complete(String[] words) {
        String notEmptyWord = findLastNotEmptyWord(words);
        try {
            return scan(notEmptyWord);
        } catch (IOException e) {
            return Collections.emptyList();
        }
    }

    private static List<String> scan(String path) throws IOException {
        Path filePath = Paths.get(path);
        if (Files.exists(filePath)) {
            if (Files.isDirectory(filePath)) {
                return listFiles(filePath, "");
            } else {
                return Collections.singletonList(path);
            }
        } else {
            int lastIndexOfPathSeparator = path.lastIndexOf(File.separator);
            if (lastIndexOfPathSeparator > -1) {
                String dir = path.substring(0, lastIndexOfPathSeparator + 1);
                String filePrefix = path.substring(lastIndexOfPathSeparator + 1);
                return listFiles(Paths.get(dir), filePrefix);
            } else {
                return Collections.emptyList();
            }
        }
    }

    private static List<String> listFiles(Path path, String prefix) throws IOException {
        try (Stream<Path> stream = Files.list(path)) {
            return stream.filter(it -> it.getFileName().toString().startsWith(prefix))
                    .map(Path::toString)
                    .collect(Collectors.toList());
        }
    }
}
