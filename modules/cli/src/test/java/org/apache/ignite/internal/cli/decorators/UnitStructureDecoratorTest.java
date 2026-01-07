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

package org.apache.ignite.internal.cli.decorators;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

import java.util.List;
import org.apache.ignite.rest.client.model.UnitEntry;
import org.apache.ignite.rest.client.model.UnitFile;
import org.apache.ignite.rest.client.model.UnitFolder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class UnitStructureDecoratorTest {

    @Test
    @DisplayName("Tree view should display folder with files")
    void treeViewWithFiles() {
        // Given
        UnitFolder folder = createFolderWithFiles("test-unit",
                createFile("file1.txt", 100),
                createFile("file2.txt", 200));

        UnitStructureDecorator decorator = new UnitStructureDecorator(false);

        // When
        String output = decorator.decorate(folder).toTerminalString();

        // Then
        assertThat(output, containsString("test-unit"));
        assertThat(output, containsString("file1.txt"));
        assertThat(output, containsString("file2.txt"));
        assertThat(output, containsString("100 B"));
        assertThat(output, containsString("200 B"));
        assertThat(output, containsString("+--"));
        assertThat(output, containsString("\\--"));
    }

    @Test
    @DisplayName("Tree view should display nested folders")
    void treeViewWithNestedFolders() {
        // Given
        UnitFolder subFolder = createFolderWithFiles("subfolder",
                createFile("nested.txt", 50));

        UnitEntry subFolderEntry = new UnitEntry();
        subFolderEntry.setActualInstance(subFolder);

        UnitFolder rootFolder = new UnitFolder()
                .type(UnitFolder.TypeEnum.FOLDER)
                .name("root")
                .children(List.of(subFolderEntry));

        UnitStructureDecorator decorator = new UnitStructureDecorator(false);

        // When
        String output = decorator.decorate(rootFolder).toTerminalString();

        // Then
        assertThat(output, containsString("root"));
        assertThat(output, containsString("subfolder"));
        assertThat(output, containsString("nested.txt"));
        assertThat(output, containsString("\\--"));
        assertThat(output, containsString("    \\--")); // Nested indent
    }

    @Test
    @DisplayName("Plain view should display file paths")
    void plainViewWithFiles() {
        // Given
        UnitFolder folder = createFolderWithFiles("test-unit",
                createFile("file1.txt", 100),
                createFile("file2.txt", 200));

        UnitStructureDecorator decorator = new UnitStructureDecorator(true);

        // When
        String output = decorator.decorate(folder).toTerminalString();

        // Then
        assertThat(output, containsString("file1.txt"));
        assertThat(output, containsString("file2.txt"));
        assertThat(output, containsString("100"));
        assertThat(output, containsString("200"));
        // Should not contain tree characters in plain mode
        assertThat(output, not(containsString("├──")));
        assertThat(output, not(containsString("└──")));
    }

    @Test
    @DisplayName("Plain view should display nested paths")
    void plainViewWithNestedFolders() {
        // Given
        UnitFolder subFolder = createFolderWithFiles("subfolder",
                createFile("nested.txt", 50));

        UnitEntry subFolderEntry = new UnitEntry();
        subFolderEntry.setActualInstance(subFolder);

        UnitFolder rootFolder = new UnitFolder()
                .type(UnitFolder.TypeEnum.FOLDER)
                .name("root")
                .children(List.of(subFolderEntry));

        UnitStructureDecorator decorator = new UnitStructureDecorator(true);

        // When
        String output = decorator.decorate(rootFolder).toTerminalString();

        // Then
        assertThat(output, containsString("subfolder/nested.txt"));
        assertThat(output, containsString("50"));
    }

    @Test
    @DisplayName("Should format file sizes correctly")
    void fileSizeFormatting() {
        // Given
        UnitFolder folder = createFolderWithFiles("test-unit",
                createFile("small.txt", 512),
                createFile("medium.txt", 2048),
                createFile("large.txt", 1024 * 1024 * 2));

        UnitStructureDecorator decorator = new UnitStructureDecorator(false);

        // When
        String output = decorator.decorate(folder).toTerminalString();

        // Then
        assertThat(output, containsString("512 B"));
        assertThat(output, containsString("KiB"));
        assertThat(output, containsString("MiB"));
    }

    @Test
    @DisplayName("Should handle empty folder")
    void emptyFolder() {
        // Given
        UnitFolder folder = new UnitFolder()
                .type(UnitFolder.TypeEnum.FOLDER)
                .name("empty-folder")
                .children(List.of());

        UnitStructureDecorator decorator = new UnitStructureDecorator(false);

        // When
        String output = decorator.decorate(folder).toTerminalString();

        // Then
        assertThat(output, containsString("empty-folder"));
    }

    private UnitFolder createFolderWithFiles(String name, UnitFile... files) {
        List<UnitEntry> children = new java.util.ArrayList<>();
        long totalSize = 0;

        for (UnitFile file : files) {
            UnitEntry entry = new UnitEntry();
            entry.setActualInstance(file);
            children.add(entry);
            totalSize += file.getSize();
        }

        return new UnitFolder()
                .type(UnitFolder.TypeEnum.FOLDER)
                .name(name)
                .children(children);
    }

    private UnitFile createFile(String name, long size) {
        return new UnitFile()
                .type(UnitFile.TypeEnum.FILE)
                .name(name)
                .size(size);
    }
}
