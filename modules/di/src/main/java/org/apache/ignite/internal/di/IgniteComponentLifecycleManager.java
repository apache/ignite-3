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

package org.apache.ignite.internal.di;

import static java.util.Collections.reverse;
import static java.util.concurrent.CompletableFuture.allOf;

import io.micronaut.context.ApplicationContext;
import io.micronaut.context.BeanRegistration;
import io.micronaut.inject.BeanDefinition;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.util.IgniteUtils;

/**
 * Manages the lifecycle of {@link IgniteComponent} beans obtained from a Micronaut {@link ApplicationContext}.
 *
 * <p>Components are grouped by {@link StartupPhase} and can be started per-phase.
 * Stopping is done in reverse order of startup across all phases.
 */
public class IgniteComponentLifecycleManager {
    private final Map<StartupPhase, List<IgniteComponent>> componentsByPhase;

    private final List<IgniteComponent> startedComponents = new CopyOnWriteArrayList<>();

    /**
     * Creates the lifecycle manager by collecting all {@link IgniteComponent} beans from the given context
     * and grouping them by their {@link IgniteStartupPhase} annotation.
     *
     * @param context Micronaut application context containing the component beans.
     */
    public IgniteComponentLifecycleManager(ApplicationContext context) {
        componentsByPhase = new EnumMap<>(StartupPhase.class);

        for (StartupPhase phase : StartupPhase.values()) {
            componentsByPhase.put(phase, new ArrayList<>());
        }

        Collection<IgniteComponent> allComponents = context.getBeansOfType(IgniteComponent.class);

        for (IgniteComponent component : allComponents) {
            StartupPhase phase = resolvePhase(context, component);
            componentsByPhase.get(phase).add(component);
        }
    }

    /**
     * Excludes specific component instances from being started by {@link #startPhase}.
     * Excluded components are also removed from the stop list unless explicitly added via {@link #markAsStarted}.
     *
     * <p>Use this for components that are started manually due to special ordering requirements
     * (e.g., nodeConfigRegistry must start before other beans are created).
     *
     * @param components Components to exclude (identity-based comparison).
     */
    public void exclude(IgniteComponent... components) {
        Set<IgniteComponent> excludeSet = newIdentitySet(components);

        for (List<IgniteComponent> phaseList : componentsByPhase.values()) {
            phaseList.removeIf(excludeSet::contains);
        }
    }

    /**
     * Marks components as started externally, so they will be stopped during {@link #stopAll}.
     *
     * <p>Use this for components that are excluded from {@link #startPhase} but still need lifecycle management.
     *
     * @param components Components that were started externally.
     */
    public void markAsStarted(IgniteComponent... components) {
        Collections.addAll(startedComponents, components);
    }

    private static Set<IgniteComponent> newIdentitySet(IgniteComponent... components) {
        Set<IgniteComponent> set = Collections.newSetFromMap(new IdentityHashMap<>());
        Collections.addAll(set, components);
        return set;
    }

    /**
     * Starts all components belonging to the given phase.
     *
     * @param phase The startup phase.
     * @param componentContext The component lifecycle context.
     * @return Future that completes when all components in the phase have started.
     */
    public CompletableFuture<Void> startPhase(StartupPhase phase, ComponentContext componentContext) {
        List<IgniteComponent> components = componentsByPhase.get(phase);

        CompletableFuture<?>[] futures = new CompletableFuture[components.size()];

        for (int i = 0; i < components.size(); i++) {
            IgniteComponent component = components.get(i);
            startedComponents.add(component);
            futures[i] = component.startAsync(componentContext);
        }

        return allOf(futures);
    }

    /**
     * Stops all started components in reverse order of startup.
     * Calls {@link IgniteComponent#beforeNodeStop()} on each component first,
     * then {@link IgniteComponent#stopAsync(ComponentContext)}.
     *
     * @param componentContext The component lifecycle context.
     * @return Future that completes when all components have stopped.
     */
    public CompletableFuture<Void> stopAll(ComponentContext componentContext) {
        List<IgniteComponent> components = new ArrayList<>(startedComponents);
        reverse(components);

        return IgniteUtils.stopAsync(componentContext, components);
    }

    /**
     * Returns the components assigned to the given phase (unmodifiable view for testing).
     *
     * @param phase The startup phase.
     * @return List of components in that phase.
     */
    public List<IgniteComponent> componentsForPhase(StartupPhase phase) {
        return List.copyOf(componentsByPhase.get(phase));
    }

    /**
     * Returns all started components in startup order.
     *
     * @return List of started components.
     */
    public List<IgniteComponent> startedComponents() {
        return List.copyOf(startedComponents);
    }

    private static StartupPhase resolvePhase(ApplicationContext context, IgniteComponent component) {
        // First, try to get the phase from the BeanRegistration for this specific instance.
        // Using findBeanRegistration(instance) instead of findBeanDefinition(class) avoids
        // NonUniqueBeanException when multiple beans of the same type exist (e.g., two ConfigurationRegistry
        // beans with different @Named qualifiers).
        Optional<? extends BeanRegistration<? extends IgniteComponent>> registration =
                context.findBeanRegistration(component);

        if (registration.isPresent()) {
            BeanDefinition<? extends IgniteComponent> definition = registration.get().getBeanDefinition();
            Optional<StartupPhase> phase = definition
                    .findAnnotation(IgniteStartupPhase.class)
                    .flatMap(av -> av.enumValue(StartupPhase.class));
            if (phase.isPresent()) {
                return phase.get();
            }
        }

        // Fall back to class-level annotation.
        IgniteStartupPhase classAnnotation = component.getClass().getAnnotation(IgniteStartupPhase.class);
        if (classAnnotation != null) {
            return classAnnotation.value();
        }

        return StartupPhase.PHASE_1;
    }
}
