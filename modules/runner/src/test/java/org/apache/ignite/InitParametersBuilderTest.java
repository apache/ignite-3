package org.apache.ignite;

import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

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
    void build_WithNullMetaStorageNodeCollection_SetsEmptyList() {
        // Arrange
        InitParametersBuilder builder = new InitParametersBuilder();

        // Act
        InitParameters result = builder
                .metaStorageNodeNames((List<String>) null)
                .clusterName("TestCluster")
                .build();

        // Assert
        assertNotNull(result);
        assertTrue(result.metaStorageNodeNames().isEmpty());
    }

    @Test
    void build_WithNullCmgNodeCollection_SetsEmptyList() {
        // Arrange
        InitParametersBuilder builder = new InitParametersBuilder();

        // Act
        InitParameters result = builder
                .cmgNodeNames((List<String>) null)
                .clusterName("TestCluster")
                .build();

        // Assert
        assertNotNull(result);
        assertTrue(result.cmgNodeNames().isEmpty());
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