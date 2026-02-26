package org.apache.ignite.internal.worker;

import io.micronaut.context.annotation.Factory;
import io.micronaut.core.annotation.Creator;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.failure.FailureManager;

/**
 * Factory class for creating instances of {@link CriticalWorkerWatchdog}.
 * This factory is responsible for constructing the {@link CriticalWorkerWatchdog}
 * using required dependencies such as configuration registry, scheduler, and
 * failure manager.
 */
@Factory
public class CriticalWorkerWatchdogFactory {
    /**
     * Creates a new instance of the CriticalWorkerWatchdog.
     *
     * @param configurationRegistry The configuration registry used to retrieve system configuration.
     * @param scheduler The scheduler used for executing periodic liveness checks.
     * @param failureManager The failure manager responsible for handling critical worker failures.
     * @return A new CriticalWorkerWatchdog instance.
     */
    @Singleton
    @Inject
    public static CriticalWorkerWatchdog create(
            ConfigurationRegistry configurationRegistry,
            @Named("commonScheduler") ScheduledExecutorService scheduler,
            FailureManager failureManager
    ) {
        CriticalWorkersConfiguration configuration = configurationRegistry
                .getConfiguration(SystemLocalExtensionConfiguration.KEY)
                .system()
                .criticalWorkers();

        return new CriticalWorkerWatchdog(configuration, scheduler, failureManager);
    }
}
