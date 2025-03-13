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

package org.apache.ignite;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.List;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;

class InitParametersBuilderTest extends BaseIgniteAbstractTest {

    @Test
    void build_WithAllParametersSet_ReturnsExpectedInitParameters() {
        // Arrange
        InitParametersBuilder builder = new InitParametersBuilder();
        String clusterName = "TestCluster";
        List<String> metaStorageNodes = List.of("Node1", "Node2");
        List<String> cmgNodes = List.of("Node3", "Node4");
        String clusterConfiguration = "{config: value}";

        // Act
        InitParameters result = builder
                .metaStorageNodeNames(metaStorageNodes)
                .cmgNodeNames(cmgNodes)
                .clusterName(clusterName)
                .clusterConfiguration(clusterConfiguration)
                .build();

        // Assert
        assertNotNull(result);
        assertEquals(metaStorageNodes, result.metaStorageNodeNames());
        assertEquals(cmgNodes, result.cmgNodeNames());
        assertEquals(clusterName, result.clusterName());
        assertEquals(clusterConfiguration, result.clusterConfiguration());
    }

    @Test
    void build_WithoutClusterName_ThrowsIllegalStateException() {
        // Arrange
        InitParametersBuilder builder = new InitParametersBuilder();

        // Act & Assert
        IllegalStateException exception = assertThrows(IllegalStateException.class, builder::build);
        assertEquals("Cluster name is not set.", exception.getMessage());
    }

    @Test
    void build_WithNullClusterName_ThrowsIllegalArgumentException() {
        // Arrange
        InitParametersBuilder builder = new InitParametersBuilder();

        // Act & Assert
        //noinspection DataFlowIssue
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> builder.clusterName(null));
        assertEquals("Cluster name cannot be null or empty.", exception.getMessage());
    }

    @Test
    void build_WithBlankClusterName_ThrowsIllegalArgumentException() {
        // Arrange
        InitParametersBuilder builder = new InitParametersBuilder();

        // Act & Assert
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> builder.clusterName(" "));
        assertEquals("Cluster name cannot be null or empty.", exception.getMessage());
    }

    @Test
    void build_WithEmptyMetaStorageNodeNames_SetsEmptyList() {
        // Arrange
        InitParametersBuilder builder = new InitParametersBuilder();

        // Act
        InitParameters result = builder
                .metaStorageNodeNames()
                .clusterName("TestCluster")
                .build();

        // Assert
        assertNotNull(result);
        assertTrue(result.metaStorageNodeNames().isEmpty());
    }

    @Test
    void build_WithEmptyCmgNodeNames_SetsEmptyList() {
        // Arrange
        InitParametersBuilder builder = new InitParametersBuilder();

        // Act
        InitParameters result = builder
                .cmgNodeNames()
                .clusterName("TestCluster")
                .build();

        // Assert
        assertNotNull(result);
        assertTrue(result.cmgNodeNames().isEmpty());
    }

    @Test
    void build_WithNullMetaStorageNodeCollection_ThrowsIllegalArgumentException() {
        // Arrange
        InitParametersBuilder builder = new InitParametersBuilder();

        // Act & Assert
        //noinspection DataFlowIssue
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> builder.metaStorageNodeNames((Collection<String>) null));
        assertEquals("Meta storage node names cannot be null.", exception.getMessage());
    }

    @Test
    void build_WithNullCmgNodeCollection_ThrowsIllegalArgumentException() {
        // Arrange
        InitParametersBuilder builder = new InitParametersBuilder();

        // Act & Assert
        //noinspection DataFlowIssue
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> builder.cmgNodeNames((Collection<String>) null));
        assertEquals("CMG node names cannot be null.", exception.getMessage());
    }

    @Test
    void build_WithNullClusterConfiguration_SetsNull() {
        // Arrange
        InitParametersBuilder builder = new InitParametersBuilder();
        String clusterName = "TestCluster";

        // Act
        InitParameters result = builder
                .clusterName(clusterName)
                .clusterConfiguration(null)
                .build();

        // Assert
        assertNotNull(result);
        assertNull(result.clusterConfiguration());
    }

    @Test
    void build_WithIgniteServerMetaStorageNodes_ReturnsExpectedNames() {
        // Arrange
        InitParametersBuilder builder = new InitParametersBuilder();
        IgniteServer server1 = mock(IgniteServer.class);
        IgniteServer server2 = mock(IgniteServer.class);

        when(server1.name()).thenReturn("Node1");
        when(server2.name()).thenReturn("Node2");

        // Act
        InitParameters result = builder
                .metaStorageNodes(server1, server2)
                .clusterName("TestCluster")
                .build();

        // Assert
        assertNotNull(result);
        assertEquals(List.of("Node1", "Node2"), result.metaStorageNodeNames());
    }

    @Test
    void build_WithIgniteServerCmgNodes_ReturnsExpectedNames() {
        // Arrange
        InitParametersBuilder builder = new InitParametersBuilder();
        IgniteServer server1 = mock(IgniteServer.class);
        IgniteServer server2 = mock(IgniteServer.class);

        when(server1.name()).thenReturn("Node3");
        when(server2.name()).thenReturn("Node4");

        // Act
        InitParameters result = builder
                .cmgNodes(server1, server2)
                .clusterName("TestCluster")
                .build();

        // Assert
        assertNotNull(result);
        assertEquals(List.of("Node3", "Node4"), result.cmgNodeNames());
    }

    @Test
    void build_WithOnlyClusterName_SetsDefaults() {
        // Arrange
        InitParametersBuilder builder = new InitParametersBuilder();
        String clusterName = "TestCluster";

        // Act
        InitParameters result = builder
                .clusterName(clusterName)
                .build();

        // Assert
        assertNotNull(result);
        assertEquals(clusterName, result.clusterName());
        assertTrue(result.metaStorageNodeNames().isEmpty(), "Meta storage node names should be empty by default.");
        assertTrue(result.cmgNodeNames().isEmpty(), "CMG node names should be empty by default.");
        assertNull(result.clusterConfiguration(), "Cluster configuration should be null by default.");
    }
}
