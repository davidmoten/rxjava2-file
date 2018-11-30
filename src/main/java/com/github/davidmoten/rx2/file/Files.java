
package com.github.davidmoten.rx2.file;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.github.davidmoten.guavamini.Preconditions;
import com.github.davidmoten.rx.util.BackpressureStrategy;

import io.reactivex.Flowable;
import io.reactivex.Scheduler;
import io.reactivex.flowables.GroupedFlowable;
import io.reactivex.functions.Function;
import io.reactivex.functions.Predicate;
import io.reactivex.schedulers.Schedulers;
import rx.functions.Action0;
import rx.functions.Func0;

/**
 * Flowable utility methods related to {@link File}.
 */
public final class Files {

    public static final int DEFAULT_MAX_BYTES_PER_EMISSION = 8192;

    private Files() {
        // prevent instantiation
    }

    /**
     * Returns an {@link Flowable} that uses NIO {@link WatchService} (and a
     * dedicated thread) to push modified events to an Flowable that reads and
     * reports new sequences of bytes to a subscriber. The NIO {@link WatchService}
     * events are sampled according to <code>sampleTimeMs</code> so that lots of
     * discrete activity on a file (for example a log file with very frequent
     * entries) does not prompt an inordinate number of file reads to pick up
     * changes.
     * 
     * @param file
     *            the file to tail
     * @param startPosition
     *            start tailing file at position in bytes
     * @param sampleTimeMs
     *            sample time in millis
     * @param chunkSize
     *            max array size of each element emitted by the Flowable. Is also
     *            used as the buffer size for reading from the file. Try
     *            {@link FileFlowable#DEFAULT_MAX_BYTES_PER_EMISSION} if you don't s
     *            know what to put here.
     * @return Flowable of byte arrays
     */
    public final static Flowable<byte[]> tailFile(File file, long startPosition, long sampleTimeMs, int chunkSize) {
        Preconditions.checkNotNull(file);
        Flowable<Object> events = from(file, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_MODIFY,
                StandardWatchEventKinds.OVERFLOW)
                        // don't care about the event details, just that there
                        // is one
                        .cast(Object.class)
                        // get lines once on subscription so we tail the lines
                        // in the file at startup
                        .startWith(new Object());
        return tailFile(file, startPosition, sampleTimeMs, chunkSize, events);
    }

    /**
     * Returns an {@link Flowable} that uses given given Flowable to push modified
     * events to an Flowable that reads and reports new sequences of bytes to a
     * subscriber. The NIO {@link WatchService} MODIFY and OVERFLOW events are
     * sampled according to <code>sampleTimeMs</code> so that lots of discrete
     * activity on a file (for example a log file with very frequent entries) does
     * not prompt an inordinate number of file reads to pick up changes. File create
     * events are not sampled and are always passed through.
     * 
     * @param file
     *            the file to tail
     * @param startPosition
     *            start tailing file at position in bytes
     * @param sampleTimeMs
     *            sample time in millis for MODIFY and OVERFLOW events
     * @param chunkSize
     *            max array size of each element emitted by the Flowable. Is also
     *            used as the buffer size for reading from the file. Try
     *            {@link FileFlowable#DEFAULT_MAX_BYTES_PER_EMISSION} if you don't
     *            know what to put here.
     * @param events
     *            trigger a check for file changes. Use
     *            {@link Flowable#interval(long, TimeUnit)} for example.
     * @return Flowable of byte arrays
     */
    public final static Flowable<byte[]> tailFile(File file, long startPosition, long sampleTimeMs, int chunkSize,
            Flowable<?> events) {
        Preconditions.checkNotNull(file);
        return sampleModifyOrOverflowEventsOnly(events, sampleTimeMs)
                // tail file triggered by events
                .compose(x -> new FlowableFileTailer(x, file, startPosition, chunkSize));
    }

    /**
     * Returns an {@link Flowable} that uses NIO {@link WatchService} (and a
     * dedicated thread) to push modified events to an Flowable that reads and
     * reports new lines to a subscriber. The NIO WatchService MODIFY and OVERFLOW
     * events are sampled according to <code>sampleTimeMs</code> so that lots of
     * discrete activity on a file (for example a log file with very frequent
     * entries) does not prompt an inordinate number of file reads to pick up
     * changes. File create events are not sampled and are always passed through.
     * 
     * @param file
     *            the file to tail
     * @param startPosition
     *            start tailing file at position in bytes
     * @param sampleTimeMs
     *            sample time in millis for MODIFY and OVERFLOW events
     * @param charset
     *            the character set to use to decode the bytes to a string
     * @return Flowable of strings
     */
    public final static Flowable<String> tailTextFile(File file, long startPosition, long sampleTimeMs,
            Charset charset) {
        return toLines(tailFile(file, startPosition, sampleTimeMs, DEFAULT_MAX_BYTES_PER_EMISSION), charset);
    }

    /**
     * Returns an {@link Flowable} of String that uses the given events stream to
     * trigger checks on file change so that new lines can be read and emitted.
     * 
     * @param file
     *            the file to tail, cannot be null
     * @param startPosition
     *            start tailing file at position in bytes
     * @param chunkSize
     *            max array size of each element emitted by the Flowable. Is also
     *            used as the buffer size for reading from the file. Try
     *            {@link FileFlowable#DEFAULT_MAX_BYTES_PER_EMISSION} if you don't
     *            know what to put here.
     * @param charset
     *            the character set to use to decode the bytes to a string
     * @param events
     *            trigger a check for file changes. Use
     *            {@link Flowable#interval(long, TimeUnit)} for example.
     * @return Flowable of strings
     */
    public final static Flowable<String> tailTextFile(File file, long startPosition, int chunkSize, Charset charset,
            Flowable<?> events) {
        Preconditions.checkNotNull(file);
        Preconditions.checkNotNull(charset);
        Preconditions.checkNotNull(events);
        return toLines(events.compose(x -> new FlowableFileTailer(x, file, startPosition, chunkSize)) //
                .onBackpressureBuffer(), charset);
    }

    /**
     * Returns an {@link Flowable} of {@link WatchEvent}s from a
     * {@link WatchService}.
     * 
     * @param watchService
     *            WatchService to generate events from
     * @param scheduler
     *            schedules polls of the watchService
     * @param pollDuration
     *            duration of each poll
     * @param pollDurationUnit
     *            time unit for the duration of each poll
     * @param pollInterval
     *            interval between polls of the watchService
     * @param pollIntervalUnit
     *            time unit for the interval between polls
     * @param backpressureStrategy
     *            backpressures strategy to apply
     * @return an Flowable of WatchEvents from watchService
     */
    public final static Flowable<WatchEvent<?>> from(WatchService watchService, Scheduler scheduler, long pollDuration,
            TimeUnit pollDurationUnit, long pollInterval, TimeUnit pollIntervalUnit,
            BackpressureStrategy backpressureStrategy) {
        Preconditions.checkNotNull(watchService);
        Preconditions.checkNotNull(scheduler);
        Preconditions.checkNotNull(pollDurationUnit);
        Preconditions.checkNotNull(backpressureStrategy);
        Flowable<WatchEvent<?>> o = new FlowableWatchServiceEvents(watchService, scheduler, pollDuration,
                pollDurationUnit, pollInterval, pollIntervalUnit);
        if (backpressureStrategy == BackpressureStrategy.BUFFER) {
            return o.onBackpressureBuffer();
        } else if (backpressureStrategy == BackpressureStrategy.DROP)
            return o.onBackpressureDrop();
        else if (backpressureStrategy == BackpressureStrategy.LATEST)
            return o.onBackpressureLatest();
        else
            throw new RuntimeException("unrecognized backpressureStrategy " + backpressureStrategy);
    }

    /**
     * Returns an {@link Flowable} of {@link WatchEvent}s from a
     * {@link WatchService}.
     * 
     * @param watchService
     *            {@link WatchService} to generate events for
     * @return Flowable of watch events from the watch service
     */
    public final static Flowable<WatchEvent<?>> from(WatchService watchService) {
        return from(watchService, Schedulers.trampoline(), Long.MAX_VALUE, TimeUnit.MILLISECONDS, 0,
                TimeUnit.SECONDS, BackpressureStrategy.BUFFER);
    }

    /**
     * If file does not exist at subscribe time then is assumed to not be a
     * directory. If the file is not a directory (bearing in mind the aforesaid
     * assumption) then a {@link WatchService} is set up on its parent and
     * {@link WatchEvent}s of the given kinds are filtered to concern the file in
     * question. If the file is a directory then a {@link WatchService} is set up on
     * the directory and all events are passed through of the given kinds.
     * 
     * @param file
     *            file to watch
     * @param kinds
     *            event kinds to watch for and emit
     * @return Flowable of watch events
     */
    @SafeVarargs
    public final static Flowable<WatchEvent<?>> from(final File file, Kind<?>... kinds) {
        return from(file, null, kinds);
    }

    /**
     * If file does not exist at subscribe time then is assumed to not be a
     * directory. If the file is not a directory (bearing in mind the aforesaid
     * assumption) then a {@link WatchService} is set up on its parent and
     * {@link WatchEvent}s of the given kinds are filtered to concern the file in
     * question. If the file is a directory then a {@link WatchService} is set up on
     * the directory and all events are passed through of the given kinds.
     * 
     * @param file
     *            file to watch
     * @param kinds
     *            event kinds to watch for and emit
     * @return Flowable of watch events
     */
    public final static Flowable<WatchEvent<?>> from(final File file, List<Kind<?>> kinds) {
        return from(file, null, kinds.toArray(new Kind<?>[] {}));
    }

    /**
     * If file does not exist at subscribe time then is assumed to not be a
     * directory. If the file is not a directory (bearing in mind the aforesaid
     * assumption) then a {@link WatchService} is set up on its parent and
     * {@link WatchEvent}s of the given kinds are filtered to concern the file in
     * question. If the file is a directory then a {@link WatchService} is set up on
     * the directory and all events are passed through of the given kinds.
     * 
     * @param file
     *            file to generate watch events from
     * @param onWatchStarted
     *            called when WatchService is created
     * @param kinds
     *            kinds of watch events to register for
     * @return Flowable of watch events
     */
    public final static Flowable<WatchEvent<?>> from(final File file, final Action0 onWatchStarted, Kind<?>... kinds) {
        return watchService(file, kinds)
                // when watch service created call onWatchStarted
                .doOnNext(w -> {
                    if (onWatchStarted != null)
                        onWatchStarted.call();
                })
                // emit events from the WatchService
                .flatMap(TO_WATCH_EVENTS)
                // restrict to events related to the file
                .filter(onlyRelatedTo(file));
    }

    /**
     * Creates a {@link WatchService} on subscribe for the given file and event
     * kinds.
     * 
     * @param file
     *            the file to watch
     * @param kinds
     *            event kinds to watch for
     * @return Flowable of watch events
     */
    @SafeVarargs
    public final static Flowable<WatchService> watchService(final File file, final Kind<?>... kinds) {
        return Flowable.defer(new Func0<Flowable<WatchService>>() {

            @Override
            public Flowable<WatchService> call() {
                try {
                    final Path path = getBasePath(file);
                    WatchService watchService = path.getFileSystem().newWatchService();
                    path.register(watchService, kinds);
                    return Flowable.just(watchService);
                } catch (Exception e) {
                    return Flowable.error(e);
                }
            }
        });

    }

    private final static Path getBasePath(final File file) {
        final Path path;
        if (file.exists() && file.isDirectory())
            path = Paths.get(file.toURI());
        else
            path = Paths.get(file.getParentFile().toURI());
        return path;
    }

    /**
     * Returns true if and only if the path corresponding to a WatchEvent represents
     * the given file. This will be the case for Create, Modify, Delete events.
     * 
     * @param file
     *            the file to restrict events to
     * @return predicate
     */
    private final static Predicate<WatchEvent<?>> onlyRelatedTo(final File file) {
        return new Predicate<WatchEvent<?>>() {

            @Override
            public boolean test(WatchEvent<?> event) {

                final boolean ok;
                if (file.isDirectory())
                    ok = true;
                else if (StandardWatchEventKinds.OVERFLOW.equals(event.kind()))
                    ok = true;
                else {
                    Object context = event.context();
                    if (context != null && context instanceof Path) {
                        Path p = (Path) context;
                        Path basePath = getBasePath(file);
                        File pFile = new File(basePath.toFile(), p.toString());
                        ok = pFile.getAbsolutePath().equals(file.getAbsolutePath());
                    } else
                        ok = false;
                }
                return ok;
            }
        };
    }

    private static Flowable<String> toLines(Flowable<byte[]> bytes, Charset charset) {
        return com.github.davidmoten.rx2.Strings.split(com.github.davidmoten.rx2.Strings.decode(bytes, charset), "\n");
    }

    private final static Function<WatchService, Flowable<WatchEvent<?>>> TO_WATCH_EVENTS = new Function<WatchService, Flowable<WatchEvent<?>>>() {

        @Override
        public Flowable<WatchEvent<?>> apply(WatchService watchService) {
            return from(watchService);
        }
    };

    private static Flowable<Object> sampleModifyOrOverflowEventsOnly(Flowable<?> events, final long sampleTimeMs) {
        return events
                // group by true if is modify or overflow, false otherwise
                .groupBy(IS_MODIFY_OR_OVERFLOW)
                // only sample if is modify or overflow
                .flatMap(sampleIfTrue(sampleTimeMs));
    }

    private static Function<GroupedFlowable<Boolean, ?>, Flowable<?>> sampleIfTrue(final long sampleTimeMs) {
        return new Function<GroupedFlowable<Boolean, ?>, Flowable<?>>() {

            @Override
            public Flowable<?> apply(GroupedFlowable<Boolean, ?> group) {
                // if is modify or overflow WatchEvent
                if (group.getKey())
                    return group.sample(sampleTimeMs, TimeUnit.MILLISECONDS);
                else
                    return group;
            }
        };
    }

    private static Function<Object, Boolean> IS_MODIFY_OR_OVERFLOW = new Function<Object, Boolean>() {

        @Override
        public Boolean apply(Object event) {
            if (event instanceof WatchEvent) {
                WatchEvent<?> w = (WatchEvent<?>) event;
                String kind = w.kind().name();
                if (kind.equals(StandardWatchEventKinds.ENTRY_MODIFY.name())
                        || kind.equals(StandardWatchEventKinds.OVERFLOW.name())) {
                    return true;
                } else
                    return false;
            } else
                return false;
        }
    };

    public static WatchEventsBuilder from(File file) {
        return new WatchEventsBuilder(file);
    }

    public static final class WatchEventsBuilder {
        private final File file;
        private Optional<Scheduler> scheduler = Optional.empty();
        private long pollInterval = 0;
        private TimeUnit pollIntervalUnit = TimeUnit.MILLISECONDS;
        private Optional<Long> pollDuration = Optional.empty();
        private TimeUnit pollDurationUnit = TimeUnit.MILLISECONDS;
        private final List<Kind<?>> kinds = new ArrayList<>();
        private BackpressureStrategy backpressureStrategy = BackpressureStrategy.BUFFER;

        private WatchEventsBuilder(File file) {
            this.file = file;
        }

        public WatchEventsBuilder scheduler(Scheduler scheduler) {
            this.scheduler = Optional.of(scheduler);
            return this;
        }

        public WatchEventsBuilder pollInterval(long interval, TimeUnit unit) {
            this.pollInterval = interval;
            this.pollIntervalUnit = unit;
            if (!pollDuration.isPresent())
                this.pollDuration = Optional.of(0L);
            return this;
        }

        public WatchEventsBuilder pollDuration(long duration, TimeUnit unit) {
            this.pollDuration = Optional.of(duration);
            this.pollDurationUnit = unit;
            return this;
        }

        public WatchEventsBuilder kind(Kind<?> kind) {
            this.kinds.add(kind);
            return this;
        }

        public WatchEventsBuilder kinds(Kind<?>... kinds) {
            for (Kind<?> kind : kinds) {
                this.kinds.add(kind);
            }
            return this;
        }

        public WatchEventsBuilder onBackpressure(BackpressureStrategy strategy) {
            this.backpressureStrategy = strategy;
            return this;
        }

        public Flowable<WatchEvent<?>> events() {
            return watchService(file, kinds.toArray(new Kind<?>[] {}))
                    .flatMap(new Function<WatchService, Flowable<WatchEvent<?>>>() {
                        @Override
                        public Flowable<WatchEvent<?>> apply(WatchService watchService) {
                            if (!scheduler.isPresent()) {
                                if (!pollDuration.isPresent() || pollDuration.get() == 0) {
                                    scheduler = Optional.of(Schedulers.computation());
                                } else {
                                    // poll will block so don't do on
                                    // computation()
                                    scheduler = Optional.of(Schedulers.io());
                                }
                            }
                            return from(watchService, scheduler.get(), pollDuration.orElse(Long.MAX_VALUE),
                                    pollDurationUnit, pollInterval, pollIntervalUnit, backpressureStrategy);
                        }
                    });
        }

    }

    public static TailerBuilder tailer() {
        return new TailerBuilder();
    }

    public static final class TailerBuilder {

        private File file = null;
        private long startPosition = 0;
        private long sampleTimeMs = 500;
        private int chunkSize = 8192;
        private Charset charset = Charset.defaultCharset();
        private Flowable<?> source = null;
        private Action0 onWatchStarted = new Action0() {
            @Override
            public void call() {
                // do nothing
            }
        };

        private TailerBuilder() {
        }

        /**
         * The file to tail.
         * 
         * @param file
         *            file to tail
         * @return the builder (this)
         */
        public TailerBuilder file(File file) {
            this.file = file;
            return this;
        }

        public TailerBuilder file(String filename) {
            return file(new File(filename));
        }

        public TailerBuilder onWatchStarted(Action0 onWatchStarted) {
            this.onWatchStarted = onWatchStarted;
            return this;
        }

        /**
         * The startPosition in bytes in the file to commence the tail from. 0 = start
         * of file. Defaults to 0.
         * 
         * @param startPosition
         *            start position
         * @return this
         */
        public TailerBuilder startPosition(long startPosition) {
            this.startPosition = startPosition;
            return this;
        }

        /**
         * Specifies sampling to apply to the source Flowable (which could be very busy
         * if a lot of writes are occurring for example). Sampling is only applied to
         * file updates (MODIFY and OVERFLOW), file creation events are always passed
         * through. File deletion events are ignored (in fact are not requested of NIO).
         * 
         * @param sampleTimeMs
         *            sample time in ms
         * @return this
         */
        public TailerBuilder sampleTimeMs(long sampleTimeMs) {
            this.sampleTimeMs = sampleTimeMs;
            return this;
        }

        /**
         * Emissions from the tailed file will be no bigger than this.
         * 
         * @param chunkSize
         *            chunk size in bytes
         * @return this
         */
        public TailerBuilder chunkSize(int chunkSize) {
            this.chunkSize = chunkSize;
            return this;
        }

        /**
         * The charset of the file. Only used for tailing a text file.
         * 
         * @param charset
         *            charset to decode with
         * @return this
         */
        public TailerBuilder charset(Charset charset) {
            this.charset = charset;
            return this;
        }

        /**
         * The charset of the file. Only used for tailing a text file.
         * 
         * @param charset
         *            charset to decode the file with
         * @return this
         */
        public TailerBuilder charset(String charset) {
            return charset(Charset.forName(charset));
        }

        public TailerBuilder utf8() {
            return charset("UTF-8");
        }

        public TailerBuilder source(Flowable<?> source) {
            this.source = source;
            return this;
        }

        public Flowable<byte[]> tail() {

            return tailFile(file, startPosition, sampleTimeMs, chunkSize, getSource());
        }

        public Flowable<String> tailText() {
            return tailTextFile(file, startPosition, chunkSize, charset, getSource());
        }

        private Flowable<?> getSource() {
            if (source == null)
                return from(file, onWatchStarted, StandardWatchEventKinds.ENTRY_CREATE,
                        StandardWatchEventKinds.ENTRY_MODIFY, StandardWatchEventKinds.OVERFLOW);
            else
                return source;

        }

    }

}
