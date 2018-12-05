package com.github.davidmoten.rx2.file;

import java.io.File;
import java.io.IOException;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import io.reactivex.Flowable;
import io.reactivex.schedulers.TestScheduler;
import io.reactivex.subscribers.TestSubscriber;

public class FilesTest {

    @Test
    public void testEvents() throws InterruptedException, IOException {
        File file = new File("target/testEvents.txt");
        file.delete();
        AtomicInteger errors = new AtomicInteger();
        TestScheduler scheduler = new TestScheduler();
        TestSubscriber<WatchEvent<?>> ts = Files //
                .from(file) //
                .scheduler(scheduler) //
                .pollInterval(1, TimeUnit.MINUTES) //
                .events() //
                .doOnNext(System.out::println) //
                .take(3) //
                .doOnError(e -> errors.incrementAndGet()) //
                .test();
        ts.assertNoValues().assertNotTerminated();
        file.createNewFile();
        Thread.sleep(300);
        scheduler.advanceTimeBy(1, TimeUnit.MINUTES);
        ts.assertValueCount(1).assertNotTerminated();
        ts.assertValueAt(0, x -> x.kind().equals(StandardWatchEventKinds.ENTRY_CREATE));
        file.delete();
        Thread.sleep(300);
        scheduler.advanceTimeBy(1, TimeUnit.MINUTES);
        ts.assertValueCount(2).assertNotTerminated();
        ts.assertValueAt(1, x -> x.kind().equals(StandardWatchEventKinds.ENTRY_DELETE));
        ts.cancel();
    }

    @Test
    public void testInterval() {
        TestScheduler scheduler = new TestScheduler();
        TestSubscriber<Long> ts = Flowable.interval(1, TimeUnit.SECONDS, scheduler) //
                .test();
        ts.assertNoValues().assertNotTerminated();
        scheduler.advanceTimeBy(1, TimeUnit.SECONDS);
        ts.assertValues(0L).assertNotTerminated();
        scheduler.advanceTimeBy(1, TimeUnit.SECONDS);
        ts.assertValues(0L, 1L).assertNotTerminated();
    }

    public static void main(String[] args) throws InterruptedException {
        Files.tailer().file("/home/dxm/ais.txt").tailText() //
                .doOnNext(x -> System.out.println(x)).subscribe();
        Thread.sleep(10000000L);
    }

}
