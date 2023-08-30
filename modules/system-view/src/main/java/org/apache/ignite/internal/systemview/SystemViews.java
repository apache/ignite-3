package org.apache.ignite.internal.systemview;

/**
 * Provides static factory methods for system view builders.
 *
 * @see ClusterSystemView
 * @see NodeSystemView
 */
public final class SystemViews {

    private SystemViews() {

    }

    /**
     * Creates an instance of a builder to construct cluster-wide system views.
     *
     * @param <T> Type of elements returned by a system view.
     * @return Returns a builder to construct cluster-wide system views.
     */
    public static <T> ClusterSystemView.Builder<T> clusterViewBuilder() {
        return new ClusterSystemView.Builder<>();
    }

    /**
     * Creates an instance of a builder to construct node system views.
     *
     * @param <T> Type of elements returned by a system view.
     * @return Returns a builder to construct node system views.
     */
    public static <T> NodeSystemView.Builder<T> nodeViewBuilder() {
        return new NodeSystemView.Builder<>();
    }
}
