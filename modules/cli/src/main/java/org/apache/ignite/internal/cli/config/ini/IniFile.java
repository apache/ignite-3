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

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collection;
import java.util.Map;
import org.apache.ignite.internal.cli.config.exception.ConfigStoringException;

/**
 * Representation of INI file.
 */
public class IniFile {
    private final Map<String, IniSection> content;

    private final IniSection topLevelSection;

    private final File file;

    /**
     * Constructor.
     *
     * @param file ini file.
     * @throws IOException in case when provided INI file can't be parsed.
     */
    public IniFile(File file) throws IOException {
        content = new IniParser().parse(file);
        topLevelSection = content.remove(IniParser.NO_SECTION);
        this.file = file;
    }

    public synchronized IniSection getSection(String name) {
        return content.get(name);
    }

    /**
     * Returns properties stored outside any section.
     *
     * @return top-level section
     */
    public IniSection getTopLevelSection() {
        return topLevelSection;
    }

    public synchronized Collection<String> getSectionNames() {
        return content.keySet();
    }

    public synchronized Collection<IniSection> getSections() {
        return content.values();
    }

    /**
     * Store current INI file to FS file.
     */
    public void store() {
        try (OutputStream os = Files.newOutputStream(file.toPath())) {
            store(os);
        } catch (IOException e) {
            throw new ConfigStoringException("Can't store cli config file " + file.getAbsolutePath(), e);
        }
    }

    private void store(OutputStream outputStream) throws IOException {
        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8));

        // Write top-level properties first
        IniSection topLevelSection = getTopLevelSection();
        if (topLevelSection != null) {
            writeSection(bufferedWriter, topLevelSection);
        }
        for (IniSection section : getSections()) {
            if (section != topLevelSection) {
                bufferedWriter.write("[" + section.getName() + "]");
                bufferedWriter.newLine();
                writeSection(bufferedWriter, section);
            }
        }
        bufferedWriter.flush();
    }

    private void writeSection(BufferedWriter bufferedWriter, IniSection section) throws IOException {
        for (Map.Entry<String, String> sectionEntry : section.getAll().entrySet()) {
            bufferedWriter.write(sectionEntry.getKey() + " = ");
            bufferedWriter.write(sectionEntry.getValue());
            bufferedWriter.newLine();
        }
        bufferedWriter.newLine();
    }

    /**
     * Create and return new {@link IniSection} with provided name.
     *
     * @param name of section.
     * @return new section.
     */
    public synchronized IniSection createSection(String name) {
        if (content.containsKey(name)) {
            throw new SectionAlreadyExistsException(name);
        }
        IniSection iniSection = new IniSection(name);
        content.put(name, iniSection);
        return iniSection;
    }

    /**
     * Returns existing or creates a new {@link IniSection} with provided name.
     *
     * @param name Section name.
     * @return Existing or new section.
     */
    public synchronized IniSection getOrCreateSection(String name) {
        return content.computeIfAbsent(name, IniSection::new);
    }
}
