package org.apache.ignite.cli.core.repl;

import jakarta.inject.Singleton;
import java.util.Objects;
import org.apache.ignite.cli.config.Config;
import picocli.CommandLine.IDefaultValueProvider;
import picocli.CommandLine.Model.ArgSpec;
import picocli.CommandLine.PropertiesDefaultProvider;

/**
 * Implementation of {@link IDefaultValueProvider} based on {@link Session}.
 */
@Singleton
public class SessionDefaultValueProvider implements IDefaultValueProvider {
    private final Session session;
    private final IDefaultValueProvider defaultValueProvider;

    /**
     * Constructor.
     *
     * @param session session instance.
     * @param config config instance.
     */
    public SessionDefaultValueProvider(Session session, Config config) {
        this.session = session;
        this.defaultValueProvider = new PropertiesDefaultProvider(config.getProperties());
    }

    @Override
    public String defaultValue(ArgSpec argSpec) throws Exception {
        if (session.isConnectedToNode()) {
            if (Objects.equals(argSpec.descriptionKey(), "ignite.jdbc-url")) {
                return session.getJdbcUrl();
            }
        }
        return defaultValueProvider.defaultValue(argSpec);
    }
}
