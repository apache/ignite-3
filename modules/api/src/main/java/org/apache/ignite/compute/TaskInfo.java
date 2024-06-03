package org.apache.ignite.compute;

import java.util.List;

public class TaskInfo {
    private final String taskClassName;

    private final List<DeploymentUnit> units;

    public TaskInfo(String taskClassName) {
        this(taskClassName, List.of());
    }

    public TaskInfo(String taskClassName, List<DeploymentUnit> units) {
        this.taskClassName = taskClassName;
        this.units = units;
    }

    public String taskClassName() {
        return taskClassName;
    }

    public List<DeploymentUnit> units() {
        return units;
    }
}
