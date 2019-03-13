package com.github.davidmoten.rx2.file;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.StandardWatchEventKinds;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.github.davidmoten.guavamini.Lists;

import io.reactivex.observers.TestObserver;
import io.reactivex.schedulers.Schedulers;
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
        System.out.println("checkEvents waitMs=" + waitMs);
        File file = new File("target/testEvents.txt");
        file.delete();
        AtomicInteger errors = new AtomicInteger();
        TestScheduler scheduler = new TestScheduler();
        TestObserver<String> ts = Files //
                .events(file) //
                .nonBlocking() //
                .pollInterval(1, TimeUnit.MINUTES, scheduler) //
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
        System.out.println("os.name=" + System.getProperty("os.name"));
        if (System.getProperty("os.name").toLowerCase().contains("win")) {
            System.out.println("ignoring test because Windows is problematic in detecting file change events");
            return;
        }
        try {
            checkTailFile(100);
        } catch (AssertionError e) {
            // fallback to 30s waits because OSX can be really slow
            // see
            // https://stackoverflow.com/questions/9588737/is-java-7-watchservice-slow-for-anyone-else
            checkTailFile(MAX_WAIT_MS);
        }
    }
    
    @Test
    public void testTailFileBlocking() throws InterruptedException, FileNotFoundException {
        System.out.println("os.name=" + System.getProperty("os.name"));
        if (System.getProperty("os.name").toLowerCase().contains("win")) {
            System.out.println("ignoring test because Windows is problematic in detecting file change events");
            return;
        }
        try {
            checkTailFileBlocking(100);
        } catch (AssertionError e) {
            // fallback to 30s waits because OSX can be really slow
            // see
            // https://stackoverflow.com/questions/9588737/is-java-7-watchservice-slow-for-anyone-else
            checkTailFileBlocking(MAX_WAIT_MS);
        }
    }

    private void checkTailFile(long waitMs) throws InterruptedException, FileNotFoundException {
        System.out.println("checkTailFile waitMs=" + waitMs);
        File file = new File("target/lines.txt");
        file.delete();
        List<String> lines = new CopyOnWriteArrayList<>();
        TestSubscriber<String> ts = Files //
                .tailLines(file) //
                .nonBlocking() //
                .pollingInterval(50, TimeUnit.MILLISECONDS) //
                .build() //
                .doOnNext(x -> lines.add(x)) //
                .test();
        checkChangesAreDetected(waitMs, file, ts);
    }
    
    private void checkTailFileBlocking(long waitMs) throws InterruptedException, FileNotFoundException {
        System.out.println("checkTailFile waitMs=" + waitMs);
        File file = new File("target/lines.txt");
        file.delete();
        List<String> lines = new CopyOnWriteArrayList<>();
        TestSubscriber<String> ts = Files //
                .tailLines(file) //
                .blocking() //
                .build() //
                .subscribeOn(Schedulers.io()) //
                .doOnNext(x -> lines.add(x)) //
                .doOnNext(System.out::println) //
                .test();
        checkChangesAreDetected(waitMs, file, ts);
        System.out.println("lines="+ lines);
    }


    private void checkChangesAreDetected(long waitMs, File file, TestSubscriber<String> ts)
            throws InterruptedException, FileNotFoundException {
        try {
            Thread.sleep(waitMs);
            try (PrintWriter out = new PrintWriter(file)) {
                out.println("a");
                // help windows know that the file has changed
                file.setLastModified(System.currentTimeMillis());
                out.flush();
                // help windows some more
                file.getParentFile().list();
                ts.awaitCount(1);

                Thread.sleep(waitMs);
                ts.assertValues("a");
                out.println("b");
                // help windows know that the file has changed
                file.setLastModified(System.currentTimeMillis());
                out.flush();

                // help windows some more
                file.getParentFile().list();
                Thread.sleep(waitMs);
                ts.awaitCount(2);
                ts.assertValues("a", "b");
            }
        } finally {
            // stop tailing
            ts.cancel();
        }
    }

    public static void main(String[] args) {
        Files.tailLines(new File("/home/dxm/temp.txt")).blocking().build().forEach(System.out::println);
    }
    
}
