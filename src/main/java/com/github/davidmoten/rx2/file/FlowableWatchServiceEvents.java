package com.github.davidmoten.rx2.file;

import java.nio.file.WatchEvent;
import java.nio.file.WatchService;
import java.util.concurrent.TimeUnit;

import org.reactivestreams.Subscriber;

import io.reactivex.Flowable;
import io.reactivex.Scheduler;

public class FlowableWatchServiceEvents extends Flowable<WatchEvent<?>> {

    public FlowableWatchServiceEvents(WatchService watchService, Scheduler scheduler, long pollDuration,
            TimeUnit pollDurationUnit, long pollInterval, TimeUnit pollIntervalUnit) {
        // TODO Auto-generated constructor stub
    }

    @Override
    protected void subscribeActual(Subscriber<? super WatchEvent<?>> s) {
        // TODO Auto-generated method stub

    }

}
