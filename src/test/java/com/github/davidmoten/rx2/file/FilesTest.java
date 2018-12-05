package com.github.davidmoten.rx2.file;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import io.reactivex.Flowable;
import io.reactivex.schedulers.TestScheduler;
import io.reactivex.subscribers.TestSubscriber;

public class FilesTest {

    @Test
    public void testEventsWithTestScheduler() throws InterruptedException, IOException {
        File file = new File("target/testEvents.txt");
        file.delete();
        AtomicInteger errors = new AtomicInteger();
        TestScheduler scheduler = new TestScheduler();
        TestSubscriber<WatchEvent<?>> ts = Files //
                .events(file) //
                .scheduler(scheduler) //
                .pollInterval(1, TimeUnit.MINUTES) //
                .build() //
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

    @Test
    public void testTailFile() throws InterruptedException, FileNotFoundException {
        File file = new File("target/lines.txt");
        file.delete();
        List<String> lines = new CopyOnWriteArrayList<>();
        TestSubscriber<String> ts = Files //
                .tailLines(file) //
                .pollingInterval(50, TimeUnit.MILLISECONDS) //
                .build() //
                .doOnNext(x -> lines.add(x)) //
                .test();
        Thread.sleep(100);
        try (PrintWriter out = new PrintWriter(file)) {
            out.println("a");
            out.flush();
            Thread.sleep(100);
            ts.assertValuesOnly("a");
            out.println("b");
            out.flush();
            Thread.sleep(100);
            ts.assertValuesOnly("a", "b");
        }
        // stop tailing
        ts.cancel();
    }

}
