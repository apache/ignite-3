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

package org.apache.ignite.internal.placementdriver.leases;

import static java.util.UUID.randomUUID;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import java.util.UUID;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;
import org.apache.ignite.internal.util.io.IgniteUnsafeDataInput;
import org.apache.ignite.internal.util.io.IgniteUnsafeDataOutput;
import org.junit.jupiter.api.Test;

class NodesDictionaryTest {
    private final NodesDictionary dictionary = new NodesDictionary();

    @Test
    void putsNames() {
        dictionary.putName("a");
        dictionary.putName("b");

        assertThat(dictionary.getName(0), is("a"));
        assertThat(dictionary.getName(1), is("b"));
    }

    @Test
    void puttingNewNameReturnsNewIndex() {
        assertThat(dictionary.putName("a"), is(0));
        assertThat(dictionary.putName("b"), is(1));
    }

    @Test
    void puttingExistingNameReturnsSameIndex() {
        dictionary.putName("a");

        assertThat(dictionary.putName("a"), is(0));
    }

    @Test
    void putsNodes() {
        UUID id1 = randomUUID();
        UUID id2 = randomUUID();
        dictionary.putNode(id1, "a");
        dictionary.putNode(id2, "b");

        assertThat(dictionary.getName(0), is("a"));
        assertThat(dictionary.getName(1), is("b"));
        assertThat(dictionary.getNodeId(0), is(id1));
        assertThat(dictionary.getNodeId(1), is(id2));
        assertThat(dictionary.getNodeName(0), is("a"));
        assertThat(dictionary.getNodeName(1), is("b"));
    }

    @Test
    void puttingNewNodeReturnsNewIndex() {
        assertThat(dictionary.putNode(randomUUID(), "a"), is(0));
        assertThat(dictionary.putNode(randomUUID(), "b"), is(1));
    }

    @Test
    void puttingExistingNodeReturnsSameIndex() {
        UUID id = randomUUID();
        dictionary.putNode(id, "a");

        assertThat(dictionary.putNode(id, "a"), is(0));
    }

    @Test
    void supportsNodesWithSameName() {
        UUID id1 = randomUUID();
        UUID id2 = randomUUID();
        dictionary.putNode(id1, "a");
        dictionary.putNode(id2, "a");

        assertThat(dictionary.getName(0), is("a"));
        assertThat(dictionary.getNodeId(0), is(id1));
        assertThat(dictionary.getNodeId(1), is(id2));
        assertThat(dictionary.getNodeName(0), is("a"));
        assertThat(dictionary.getNodeName(1), is("a"));
    }

    @Test
    void putNodeAddsName() {
        dictionary.putNode(randomUUID(), "a");

        assertThat(dictionary.putName("a"), is(0));
    }

    @Test
    void namesAndNodesHaveIndependentIndexes() {
        assertThat(dictionary.putName("a"), is(0));
        assertThat(dictionary.putNode(randomUUID(), "b"), is(0));
    }

    @Test
    void nameCountIsZeroInitially() {
        assertThat(dictionary.nameCount(), is(0));
    }

    @Test
    void nameCountIsIncreasedWhenPuttingNewName() {
        dictionary.putName("a");
        assertThat(dictionary.nameCount(), is(1));

        dictionary.putName("b");
        assertThat(dictionary.nameCount(), is(2));
    }

    @Test
    void nameCountIsNotIncreasedWhenPuttingExistingName() {
        dictionary.putName("a");
        dictionary.putName("a");

        assertThat(dictionary.nameCount(), is(1));
    }

    @Test
    void nameCountIsIncreasedWhenPuttingNewNode() {
        dictionary.putNode(randomUUID(), "a");
        assertThat(dictionary.nameCount(), is(1));

        dictionary.putNode(randomUUID(), "b");
        assertThat(dictionary.nameCount(), is(2));
    }

    @Test
    void nameCountIsNotIncreasedWhenPuttingExistingNode() {
        UUID id = randomUUID();
        dictionary.putNode(id, "a");
        dictionary.putNode(id, "a");

        assertThat(dictionary.nameCount(), is(1));
    }

    @Test
    void serializationAndDeserialization() throws Exception {
        dictionary.putName("stray-name");
        dictionary.putNode(randomUUID(), "node1");
        dictionary.putNode(randomUUID(), "node2");
        dictionary.putNode(randomUUID(), "node2");

        IgniteDataOutput out = new IgniteUnsafeDataOutput(100);
        dictionary.writeTo(out);
        byte[] bytes = out.array();

        IgniteDataInput in = new IgniteUnsafeDataInput(bytes);
        NodesDictionary restoredDict = NodesDictionary.readFrom(in);

        assertThat(restoredDict, equalTo(dictionary));
    }
}
