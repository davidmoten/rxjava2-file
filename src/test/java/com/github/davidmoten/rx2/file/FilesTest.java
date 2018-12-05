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

import io.reactivex.schedulers.TestScheduler;
import io.reactivex.subscribers.TestSubscriber;

public class FilesTest {

    @Test
    public void testEventsWithTestScheduler() throws InterruptedException, IOException {
        try {
            checkEvents(300);
        } catch (AssertionError e) {
            // fallback to 10s waits because OSX can be really slow
            // see
            // https://stackoverflow.com/questions/9588737/is-java-7-watchservice-slow-for-anyone-else
            checkEvents(10000);
        }
    }

    private void checkEvents(long waitMs) throws IOException, InterruptedException {
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
        Thread.sleep(waitMs);
        scheduler.advanceTimeBy(1, TimeUnit.MINUTES);
        ts.assertValueCount(1).assertNotTerminated();
        ts.assertValueAt(0, x -> x.kind().equals(StandardWatchEventKinds.ENTRY_CREATE));
        file.delete();
        Thread.sleep(waitMs);
        scheduler.advanceTimeBy(1, TimeUnit.MINUTES);
        ts.assertValueCount(2).assertNotTerminated();
        ts.assertValueAt(1, x -> x.kind().equals(StandardWatchEventKinds.ENTRY_DELETE));
        ts.cancel();
    }

    @Test
    public void testTailFile() throws InterruptedException, FileNotFoundException {
        try {
            checkTailFile(100);
        } catch (AssertionError e) {
            // fallback to 10s waits because OSX can be really slow
            // see
            // https://stackoverflow.com/questions/9588737/is-java-7-watchservice-slow-for-anyone-else
            checkTailFile(10000);
        }
    }

    private void checkTailFile(long waitMs) throws InterruptedException, FileNotFoundException {
        File file = new File("target/lines.txt");
        file.delete();
        List<String> lines = new CopyOnWriteArrayList<>();
        TestSubscriber<String> ts = Files //
                .tailLines(file) //
                .pollingInterval(50, TimeUnit.MILLISECONDS) //
                .build() //
                .doOnNext(x -> lines.add(x)) //
                .test();
        try {
            Thread.sleep(waitMs);
            try (PrintWriter out = new PrintWriter(file)) {
                out.println("a");
                out.flush();
                Thread.sleep(waitMs);
                ts.assertValuesOnly("a");
                out.println("b");
                out.flush();
                Thread.sleep(waitMs);
                ts.assertValuesOnly("a", "b");
            }
        } finally {
            // stop tailing
            ts.cancel();
        }
    }

}
