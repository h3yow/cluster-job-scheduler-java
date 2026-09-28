package com.github.h3yow.clusterjobschedulerjava.scheduler.model;

/** 作业优先级：order 越小越优先。 */
public enum JobPriority {
    HIGH(0),
    NORMAL(1),
    LOW(2);

    private final int order;

    JobPriority(int order) {
        this.order = order;
    }

    public int order() {
        return order;
    }
}
