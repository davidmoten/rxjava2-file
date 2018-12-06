package com.github.davidmoten.rx2.file;

import static org.junit.Assert.assertTrue;

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

import com.github.davidmoten.guavamini.Lists;

import io.reactivex.schedulers.TestScheduler;
import io.reactivex.subscribers.TestSubscriber;

public class FilesTest {

    private static final long MAX_WAIT_MS = 30000;

    @Test
    public void testEventsWithTestScheduler() throws InterruptedException, IOException {
        try {
            checkEvents(300);
        } catch (AssertionError e) {
            // fallback to 30s waits because OSX can be really slow
            // see
            // https://stackoverflow.com/questions/9588737/is-java-7-watchservice-slow-for-anyone-else
            checkEvents(MAX_WAIT_MS);
        }
    }

    private void checkEvents(long waitMs) throws IOException, InterruptedException {
        System.out.println("checkEvents waitMs="+ waitMs);
        File file = new File("target/testEvents.txt");
        file.delete();
        AtomicInteger errors = new AtomicInteger();
        TestScheduler scheduler = new TestScheduler();
        TestSubscriber<String> ts = Files //
                .events(file) //
                .scheduler(scheduler) //
                .pollInterval(1, TimeUnit.MINUTES) //
                .build() //
                .doOnNext(x -> System.out.println(x.kind().name() + ", count=" + x.count())) //
                .map(x -> x.kind().name()).take(3) //
                .doOnError(e -> errors.incrementAndGet()) //
                .test();
        ts.assertNoValues().assertNotTerminated();
        file.createNewFile();
        Thread.sleep(waitMs);
        scheduler.advanceTimeBy(1, TimeUnit.MINUTES);
        ts.assertValueCount(1).assertNotTerminated();
        ts.assertValueAt(0, x -> x.equals(StandardWatchEventKinds.ENTRY_CREATE.name()));
        file.delete();
        Thread.sleep(waitMs);
        scheduler.advanceTimeBy(1, TimeUnit.MINUTES);
        // windows detects ENTRY_MODIFY, ENTRY_DELETE
        // linux, osx detect ENTRY_DELETE
        assertTrue(ts.values().equals(Lists.newArrayList( //
                StandardWatchEventKinds.ENTRY_CREATE.name(), //
                StandardWatchEventKinds.ENTRY_DELETE.name())) //
                || ts.values().equals(Lists.newArrayList( //
                        StandardWatchEventKinds.ENTRY_CREATE.name(), //
                        StandardWatchEventKinds.ENTRY_MODIFY.name(), //
                        StandardWatchEventKinds.ENTRY_DELETE.name())));
        ts.cancel();
    }

    @Test
    public void testTailFile() throws InterruptedException, FileNotFoundException {
        try {
            checkTailFile(100);
        } catch (AssertionError e) {
            // fallback to 30s waits because OSX can be really slow
            // see
            // https://stackoverflow.com/questions/9588737/is-java-7-watchservice-slow-for-anyone-else
            checkTailFile(MAX_WAIT_MS);
        }
    }

    private void checkTailFile(long waitMs) throws InterruptedException, FileNotFoundException {
        System.out.println("checkTailFile waitMs="+ waitMs);
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
                //help windows know that the file has changed
                file.setLastModified(System.currentTimeMillis());
                Thread.sleep(waitMs);
                ts.assertValues("a");
                out.println("b");
                out.flush();
                // help windows know that the file has changed
                file.setLastModified(System.currentTimeMillis());
                Thread.sleep(waitMs);
                ts.assertValues("a", "b");
            }
        } finally {
            // stop tailing
            ts.cancel();
        }
    }

}
