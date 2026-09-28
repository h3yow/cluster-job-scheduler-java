package com.github.h3yow.clusterjobschedulerjava.model;

/**
 * 不可变的资源量（CPU 核数 / 内存 MB）。
 */
public record Resources(long cpu, long memoryMb) {
    public static final Resources ZERO = new Resources(0, 0);

    public Resources {
        if (cpu < 0 || memoryMb < 0) {
            throw new IllegalArgumentException("resource values must be non-negative");
        }
    }

    public Resources plus(Resources other) {
        return new Resources(cpu + other.cpu, memoryMb + other.memoryMb);
    }

    public Resources minus(Resources other) {
        return new Resources(cpu - other.cpu, memoryMb - other.memoryMb);
    }

    /** 该资源量（含两维）是否能放入给定容量。 */
    public boolean fitsIn(Resources capacity) {
        return cpu <= capacity.cpu && memoryMb <= capacity.memoryMb;
    }
}
