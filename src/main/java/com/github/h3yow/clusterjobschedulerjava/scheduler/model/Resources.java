package com.github.h3yow.clusterjobschedulerjava.scheduler.model;

/** 可调度资源：CPU 核数与内存 MB。 */
public final class Resources {
    public static final Resources ZERO = new Resources(0, 0L);

    private final int cpuCores;
    private final long memoryMb;

    public Resources(int cpuCores, long memoryMb) {
        if (cpuCores < 0 || memoryMb < 0) {
            throw new IllegalArgumentException("resources must be non-negative: cpu=" + cpuCores + ", memoryMb=" + memoryMb);
        }
        this.cpuCores = cpuCores;
        this.memoryMb = memoryMb;
    }

    public int getCpuCores() {
        return cpuCores;
    }

    public long getMemoryMb() {
        return memoryMb;
    }

    /** 当前资源量是否足以容纳 requested。 */
    public boolean canFit(Resources requested) {
        return this.cpuCores >= requested.cpuCores && this.memoryMb >= requested.memoryMb;
    }

    public Resources plus(Resources other) {
        return new Resources(this.cpuCores + other.cpuCores, this.memoryMb + other.memoryMb);
    }

    public Resources minus(Resources other) {
        return new Resources(this.cpuCores - other.cpuCores, this.memoryMb - other.memoryMb);
    }

    @Override
    public String toString() {
        return "Resources(cpu=" + cpuCores + ",memoryMb=" + memoryMb + ")";
    }
}
