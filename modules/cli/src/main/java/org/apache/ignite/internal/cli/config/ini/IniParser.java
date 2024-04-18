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

package org.apache.ignite.internal.cli.config.ini;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * INI file parser.
 */
public class IniParser {
    /**
     * Section name for properties outside any section.
     */
    public static final String NO_SECTION = "NO_SECTION";

    private static final Pattern SECTION_PATTERN  = Pattern.compile("\\s*\\[([^]]*)\\]\\s*");
    private static final Pattern KEY_VALUE_PATTER = Pattern.compile("\\s*([^=]*)=(.*)");
    private static final Pattern COMMENT_LINE = Pattern.compile("^[;|#].*");


    /**
     * Parse incoming input string as {@link Map}.
     *
     * @param inputStream of INI file.
     * @return Map representation of INI file.
     * @throws IOException when parsing failed.
     */
    public Map<String, IniSection> parse(InputStream inputStream) throws IOException {
        if (inputStream == null) {
            throw new FileNotFoundException("inputStream is null");
        }

        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, UTF_8))) {
            return parseIniFile(bufferedReader);
        }
    }

    /**
     * Parse incoming file as {@link Map}.
     *
     * @param file INI file.
     * @return Map representation of INI file.
     * @throws IOException when parsing failed.
     */
    public Map<String, IniSection> parse(File file) throws IOException {
        try (InputStream input = Files.newInputStream(file.toPath())) {
            return parse(input);
        }
    }

    private Map<String, IniSection> parseIniFile(BufferedReader bufferedReader) throws IOException {
        Map<String, IniSection> map = new LinkedHashMap<>();
        IniSection currentSection = new IniSection(NO_SECTION);
        map.put(NO_SECTION, currentSection);
        String line;
        while ((line = bufferedReader.readLine()) != null) {

            Matcher commentMatcher = COMMENT_LINE.matcher(line);
            if (commentMatcher.matches()) {
                continue;
            }

            Matcher sectionMather = SECTION_PATTERN.matcher(line);
            if (sectionMather.matches()) {
                String sectionName = sectionMather.group(1).trim();
                currentSection = new IniSection(sectionName);
                map.put(sectionName, currentSection);
                continue;
            }

            Matcher keyValueMatcher = KEY_VALUE_PATTER.matcher(line);
            if (keyValueMatcher.matches()) {
                String key = keyValueMatcher.group(1).trim();
                String value = keyValueMatcher.group(2).trim();
                currentSection.setProperty(key, value);
            }
        }
        return map;
    }
}
