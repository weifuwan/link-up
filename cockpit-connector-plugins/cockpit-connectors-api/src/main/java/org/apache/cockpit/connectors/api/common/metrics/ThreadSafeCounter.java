package org.apache.cockpit.connectors.api.common.metrics;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

public class ThreadSafeCounter implements Counter, Serializable {

    private static final long serialVersionUID = 1L;

    private final String name;
    private static final AtomicLongFieldUpdater<ThreadSafeCounter> VOLATILE_VALUE_UPDATER =
            AtomicLongFieldUpdater.newUpdater(ThreadSafeCounter.class, "value");

    private volatile long value;

    public ThreadSafeCounter(String name) {
        this.name = name;
    }

    @Override
    public void inc() {
        VOLATILE_VALUE_UPDATER.incrementAndGet(this);
    }

    @Override
    public void inc(long n) {
        VOLATILE_VALUE_UPDATER.addAndGet(this, n);
    }

    @Override
    public void dec() {
        VOLATILE_VALUE_UPDATER.decrementAndGet(this);
    }

    @Override
    public void dec(long n) {
        VOLATILE_VALUE_UPDATER.addAndGet(this, -n);
    }

    @Override
    public void set(long n) {
        VOLATILE_VALUE_UPDATER.set(this, n);
    }

    @Override
    public long getCount() {
        return VOLATILE_VALUE_UPDATER.get(this);
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Unit unit() {
        return Unit.COUNT;
    }

    @Override
    public String toString() {
        return "ThreadSafeCounter{" + "name='" + name + '\'' + ", value=" + value + '}';
    }
}
