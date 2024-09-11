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

package org.apache.ignite.internal.cli.commands.treesitter.parser;

import java.util.function.Function;
import org.treesitter.TSNode;
import org.treesitter.TSTree;
import org.treesitter.TSTreeCursor;

/**
 * Builds an "index" for the provided text and abstract syntax tree.
 * The index is an array of tokens, where each token is a member of the corresponding enum.
 * It is used to map each character in the input text to a token.
 */
public class Indexer {

    /**
     * Builds an index for the provided SQL text and abstract syntax tree.
     *
     * @param text The input SQL text.
     * @param tree The abstract syntax tree.
     * @return The index.
     */
    public static SqlTokenType[] indexSql(String text, TSTree tree) {
        return index(text, tree, SqlTokenType[]::new, SqlTokenType.SPACE, SqlTokenType::fromNode);
    }

    /**
     * Builds an index for the provided JSON text and abstract syntax tree.
     *
     * @param text The input JSON text.
     * @param tree The abstract syntax tree.
     * @return The index.
     */
    public static JsonTokenType[] indexJson(String text, TSTree tree) {
        return index(text, tree, JsonTokenType[]::new, JsonTokenType.SPACE, JsonTokenType::fromNode);
    }

    /**
     * Builds an index for the provided HOCON text and abstract syntax tree.
     *
     * @param text The input HOCON text.
     * @param tree The abstract syntax tree.
     * @return The index.
     */
    public static HoconTokenType[] indexHocon(String text, TSTree tree) {
        return index(text, tree, HoconTokenType[]::new, HoconTokenType.SPACE, HoconTokenType::fromNode);
    }

    /**
     * Builds an index for the provided text and abstract syntax tree.
     *
     * @param text The input text.
     * @param tree The abstract syntax tree.
     * @param tokensArrayFactory Function to create an array of tokens.
     * @param spaceToken Token representing space.
     * @param nodeToToken Function to extract a token from the node of the tree.
     * @return The index.
     */
    private static <T> T[] index(
            String text,
            TSTree tree,
            Function<Integer, T[]> tokensArrayFactory,
            T spaceToken,
            Function<TSNode, T> nodeToToken
    ) {
        T[] tokens = tokensArrayFactory.apply(text.length());
        TSTreeCursor cursor = new TSTreeCursor(tree.getRootNode());
        for (int i = 0; i < text.length(); i++) {
            if (Character.isSpaceChar(text.charAt(i))) {
                tokens[i] = spaceToken;
                continue;
            }
            findTerminalNode(i, cursor);
            TSNode node = cursor.currentNode();
            tokens[i] = nodeToToken.apply(node);
            cursor.reset(tree.getRootNode());
        }
        return tokens;
    }

    private static void findTerminalNode(int pos, TSTreeCursor cursor) {
        int i = 0;
        while (inside(pos, cursor.currentNode()) && -1 != i) {
            i = cursor.gotoFirstChildForByte(pos);
        }

        if (!inside(pos, cursor.currentNode())) {
            if (cursor.gotoNextSibling()) {
                findTerminalNode(pos, cursor);
            } else {
                cursor.gotoParent();
            }
        }
    }

    private static boolean inside(int pos, TSNode node) {
        return pos >= node.getStartByte() && pos < node.getEndByte();
    }
}
