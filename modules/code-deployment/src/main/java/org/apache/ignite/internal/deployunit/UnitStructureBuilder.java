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

package org.apache.ignite.internal.deployunit;

import java.util.ArrayDeque;
import java.util.Deque;
import org.apache.ignite.internal.deployunit.structure.UnitFile;
import org.apache.ignite.internal.deployunit.structure.UnitFolder;

/**
 * Builder for constructing hierarchical deployment unit structures.
 *
 * <p>This class uses a stack-based approach to build a tree structure of folders and files
 * representing the content of a deployment unit. Folders can be nested by pushing and popping
 * them on the internal stack, while files are added to the current (top) folder.
 *
 * @see UnitFolder
 * @see UnitFile
 */
public class UnitStructureBuilder {
    /** Stack of folders used to build the hierarchical structure. */
    private final Deque<UnitFolder> folderStack = new ArrayDeque<>();

    /**
     * Pushes a new folder onto the stack and adds it as a child to the current folder.
     *
     * <p>If there is already a folder on the stack, the new folder is added as a child
     * to it. The new folder becomes the current folder (top of the stack) to which
     * subsequent files and folders will be added.
     *
     * @param folderName the name of the folder to push
     */
    public void pushFolder(String folderName) {
        UnitFolder folder = folderStack.peek();
        UnitFolder newFolder = new UnitFolder(folderName);
        if (folder != null) {
            folder.addChild(newFolder);
        }
        folderStack.push(newFolder);
    }

    /**
     * Pops the current folder from the stack.
     *
     * <p>The provided folder name is verified against the name of the folder being popped
     * to ensure correct nesting structure.
     *
     * @param folderName the expected name of the folder being popped (for verification)
     */
    public void popFolder(String folderName) {
        UnitFolder lastFolder = folderStack.pop();
        assert lastFolder.name().equals(folderName);
    }

    /**
     * Adds a file to the current folder.
     *
     * <p>The file is added as a child to the folder at the top of the stack.
     *
     * @param fileName the name of the file
     * @param size the size of the file in bytes
     */
    public void addFile(String fileName, long size) {
        UnitFolder folder = folderStack.peek();
        folder.addChild(new UnitFile(fileName, size));
    }

    /**
     * Builds and returns the root folder structure.
     *
     * <p>This method pops and returns the last remaining folder from the stack,
     * which should be the root of the built structure. The stack should contain
     * exactly one folder when this method is called.
     *
     * @return the root folder of the built structure
     * @throws java.util.NoSuchElementException if the stack is empty
     */
    public UnitFolder build() {
        return folderStack.pop();
    }
}
