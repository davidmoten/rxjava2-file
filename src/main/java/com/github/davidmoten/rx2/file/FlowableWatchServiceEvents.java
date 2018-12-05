package com.github.davidmoten.rx2.file;

import java.nio.file.WatchEvent;
import java.nio.file.WatchService;

import org.reactivestreams.Subscriber;

import io.reactivex.Flowable;
import io.reactivex.Scheduler;

public final class FlowableWatchServiceEvents extends Flowable<WatchEvent<?>> {

    private final WatchService watchService;
    private final Scheduler scheduler;
    private final long pollIntervalMs;

    public FlowableWatchServiceEvents(WatchService watchService, Scheduler scheduler, long pollIntervalMs
            ) {
        this.watchService = watchService;
        this.scheduler = scheduler;
        this.pollIntervalMs = pollIntervalMs;
    }

    @Override
    protected void subscribeActual(Subscriber<? super WatchEvent<?>> s) {
        // TODO Auto-generated method stub

    }

}
