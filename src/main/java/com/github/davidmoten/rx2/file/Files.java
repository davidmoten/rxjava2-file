
package com.github.davidmoten.rx2.file;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.github.davidmoten.guavamini.Preconditions;
import com.github.davidmoten.rx2.Bytes;

import io.reactivex.Flowable;
import io.reactivex.Scheduler;
import io.reactivex.flowables.GroupedFlowable;
import io.reactivex.functions.Function;
import io.reactivex.functions.Predicate;
import io.reactivex.schedulers.Schedulers;

/**
 * Flowable utility methods related to {@link File}.
 */
public final class Files {

    private static final int DEFAULT_POLLING_INTERVAL_MS = 1000;
    public static final int DEFAULT_MAX_BYTES_PER_EMISSION = 8192;
    public static final Kind<?>[] ALL_KINDS = new Kind<?>[] { StandardWatchEventKinds.ENTRY_CREATE,
            StandardWatchEventKinds.ENTRY_DELETE, StandardWatchEventKinds.ENTRY_MODIFY,
            StandardWatchEventKinds.OVERFLOW };

    private Files() {
        // prevent instantiation
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
     * @param pollingIntervalMs
     *            polling time in millis for MODIFY and OVERFLOW events and half of
     *            sample time for overflow
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
    private static Flowable<byte[]> tailBytes(File file, long startPosition, long pollingIntervalMs, int chunkSize,
            Flowable<?> events) {
        Preconditions.checkNotNull(file);
        return sampleModifyOrOverflowEventsOnly(events, pollingIntervalMs * 2)
                // tail file triggered by events
                .compose(x -> eventsToBytes(events, file, startPosition, chunkSize));
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
    private static Flowable<String> tailLines(File file, long startPosition, int chunkSize, Charset charset,
            Flowable<?> events) {
        Preconditions.checkNotNull(file);
        Preconditions.checkNotNull(charset);
        Preconditions.checkNotNull(events);
        return toLines(events.compose(x -> eventsToBytes(x, file, startPosition, chunkSize)), charset);
    }

    private static Flowable<WatchEvent<?>> events(WatchService watchService, Scheduler scheduler, long intervalMs) {
        Preconditions.checkNotNull(watchService, "watchService cannot be null");
        Preconditions.checkNotNull(scheduler, "scheduler cannot be null");
        Preconditions.checkArgument(intervalMs > 0, "intervalMs must be positive");
        return Flowable.interval(intervalMs, TimeUnit.MILLISECONDS, scheduler) //
                .flatMap(x -> {
                    WatchKey key = watchService.poll();
                    if (key != null) {
                        Flowable<WatchEvent<?>> r = Flowable.fromIterable(key.pollEvents());
                        key.reset();
                        return r;
                    } else {
                        return Flowable.empty();
                    }
                });
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
    private static Flowable<WatchEvent<?>> events(File file, Scheduler scheduler, long pollingIntervalMs,
            Kind<?>... kinds) {
        return Flowable.using(() -> watchService(file, kinds), //
                ws -> events(ws, scheduler, pollingIntervalMs)
                        // restrict to events related to the file
                        .filter(onlyRelatedTo(file)), //
                ws -> ws.close(), true);
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
     * @throws IOException
     */
    @SafeVarargs
    private static WatchService watchService(final File file, final Kind<?>... kinds) throws IOException {
        final Path path = getBasePath(file);
        WatchService watchService = path.getFileSystem().newWatchService();
        path.register(watchService, kinds);
        return watchService;
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

    private static Flowable<Object> sampleModifyOrOverflowEventsOnly(Flowable<?> events, final long sampleTimeMs) {
        return events
                // group by true if is modify or overflow, false otherwise
                .groupBy(IS_MODIFY_OR_OVERFLOW)
                // only sample if is modify or overflow
                .flatMap(sampleIfTrue(sampleTimeMs));
    }

    private static Function<GroupedFlowable<Boolean, ?>, Flowable<?>> sampleIfTrue(final long sampleTimeMs) {
        return group -> { // if is modify or overflow WatchEvent
            if (group.getKey())
                return group.sample(sampleTimeMs, TimeUnit.MILLISECONDS);
            else
                return group;
        };
    }

    private static Function<Object, Boolean> IS_MODIFY_OR_OVERFLOW = event -> {
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
    };

    public static WatchEventsBuilder events(File file) {
        return new WatchEventsBuilder(file);
    }

    public static final class WatchEventsBuilder {
        private final File file;
        private Optional<Scheduler> scheduler = Optional.empty();
        private long pollInterval = DEFAULT_POLLING_INTERVAL_MS;
        private TimeUnit pollIntervalUnit = TimeUnit.MILLISECONDS;
        private final List<Kind<?>> kinds = new ArrayList<>();

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
            return this;
        }

        /**
         * If no kind is specified then all {@link StandardWatchEventKinds} are used.
         * 
         * @param kind
         *            kind to add
         * @return this
         */
        public WatchEventsBuilder kind(Kind<?> kind) {
            this.kinds.add(kind);
            return this;
        }

        /**
         * If no kind is specified then all {@link StandardWatchEventKinds} are used.
         * 
         * @param kinds
         *            kinds to add
         * @return this
         */
        public WatchEventsBuilder kinds(Kind<?>... kinds) {
            for (Kind<?> kind : kinds) {
                this.kinds.add(kind);
            }
            return this;
        }

        public Flowable<WatchEvent<?>> build() {
            List<Kind<?>> kindsCopy = new ArrayList<>(kinds);
            if (kindsCopy.isEmpty()) {
                kindsCopy.add(StandardWatchEventKinds.ENTRY_CREATE);
                kindsCopy.add(StandardWatchEventKinds.ENTRY_DELETE);
                kindsCopy.add(StandardWatchEventKinds.ENTRY_MODIFY);
                kindsCopy.add(StandardWatchEventKinds.OVERFLOW);
            }
            return Flowable.using( //
                    () -> watchService(file, kindsCopy.toArray(new Kind<?>[] {})), //
                    ws -> Files.events(ws, scheduler.orElse(Schedulers.io()), pollIntervalUnit.toMillis(pollInterval)), //
                    ws -> ws.close(), //
                    true);
        }

    }

    public static TailerBytesBuilder tailBytes(File file) {
        return new TailerBytesBuilder(file);
    }

    public static TailerBytesBuilder tailBytes(String filename) {
        return tailBytes(new File(filename));
    }

    public static TailerLinesBuilder tailLines(File file) {
        return new TailerLinesBuilder(file);
    }

    public static TailerLinesBuilder tailLines(String filename) {
        return tailLines(new File(filename));
    }

    public static final class TailerBytesBuilder {

        private final File file;
        private long startPosition = 0;
        private int chunkSize = 8192;
        private long pollingIntervalMs = DEFAULT_POLLING_INTERVAL_MS;
        private Scheduler scheduler = Schedulers.io();
        private Flowable<?> events;

        TailerBytesBuilder(File file) {
            this.file = file;
        }

        /**
         * The startPosition in bytes in the file to commence the tail from. 0 = start
         * of file. Defaults to 0.
         * 
         * @param startPosition
         *            start position
         * @return this
         */
        public TailerBytesBuilder startPosition(long startPosition) {
            this.startPosition = startPosition;
            return this;
        }

        public TailerBytesBuilder pollingInterval(long pollingInterval, TimeUnit unit) {
            this.pollingIntervalMs = unit.toMillis(pollingInterval);
            return this;
        }

        /**
         * Emissions from the tailed file will be no bigger than this.
         * 
         * @param chunkSize
         *            chunk size in bytes
         * @return this
         */
        public TailerBytesBuilder chunkSize(int chunkSize) {
            this.chunkSize = chunkSize;
            return this;
        }

        public TailerBytesBuilder scheduler(Scheduler scheduler) {
            this.scheduler = scheduler;
            return this;
        }

        public TailerBytesBuilder events(Flowable<?> events) {
            this.events = events;
            return this;
        }

        private Flowable<?> events() {
            if (events == null) {
                return Files.events(file, scheduler, pollingIntervalMs, ALL_KINDS);
            } else {
                return events;
            }
        }

        public Flowable<byte[]> build() {
            return Files.tailBytes(file, startPosition, pollingIntervalMs, chunkSize, events());
        }

    }

    public static final class TailerLinesBuilder {

        private final File file;
        private long startPosition = 0;
        private int chunkSize = 8192;
        private Charset charset = StandardCharsets.UTF_8;
        private long pollingIntervalMs = DEFAULT_POLLING_INTERVAL_MS;
        private Scheduler scheduler = Schedulers.io();
        private Flowable<?> events;

        TailerLinesBuilder(File file) {
            this.file = file;
        }

        /**
         * The startPosition in bytes in the file to commence the tail from. 0 = start
         * of file. Defaults to 0.
         * 
         * @param startPosition
         *            start position
         * @return this
         */
        public TailerLinesBuilder startPosition(long startPosition) {
            this.startPosition = startPosition;
            return this;
        }

        public TailerLinesBuilder pollingInterval(long pollingInterval, TimeUnit unit) {
            this.pollingIntervalMs = unit.toMillis(pollingInterval);
            return this;
        }

        /**
         * Emissions from the tailed file will be no bigger than this.
         * 
         * @param chunkSize
         *            chunk size in bytes
         * @return this
         */
        public TailerLinesBuilder chunkSize(int chunkSize) {
            this.chunkSize = chunkSize;
            return this;
        }

        /**
         * The charset of the file. Only used for tailing a text file. Default is UTF-8.
         * 
         * @param charset
         *            charset to decode with
         * @return this
         */
        public TailerLinesBuilder charset(Charset charset) {
            this.charset = charset;
            return this;
        }

        /**
         * The charset of the file. Only used for tailing a text file. Default is UTF-8.
         * 
         * @param charset
         *            charset to decode the file with
         * @return this
         */
        public TailerLinesBuilder charset(String charset) {
            return charset(Charset.forName(charset));
        }

        public TailerLinesBuilder utf8() {
            return charset("UTF-8");
        }

        public TailerLinesBuilder scheduler(Scheduler scheduler) {
            this.scheduler = scheduler;
            return this;
        }

        public TailerLinesBuilder events(Flowable<?> events) {
            this.events = events;
            return this;
        }

        private Flowable<?> events() {
            if (events == null) {
                return Files.events(file, scheduler, pollingIntervalMs, ALL_KINDS);
            } else {
                return events;
            }
        }

        public Flowable<String> build() {
            return Files.tailLines(file, startPosition, chunkSize, charset, events());
        }
    }

    private static final class State {
        long position;
    }

    private static Flowable<byte[]> eventsToBytes(Flowable<?> events, File file, long startPosition, int chunkSize) {
        return Flowable.defer(() -> {
            State state = new State();
            state.position = startPosition;
            return events.flatMap(event -> eventToBytes(event, file, state, chunkSize));
        });
    }

    private static Flowable<byte[]> eventToBytes(Object event, File file, State state, int chunkSize) {
        if (event instanceof WatchEvent) {
            WatchEvent<?> w = (WatchEvent<?>) event;
            String kind = w.kind().name();
            if (kind.equals(StandardWatchEventKinds.ENTRY_CREATE.name())) {
                // if file has just been created then start from the start of the new file
                state.position = 0;
            } else if (kind.equals(StandardWatchEventKinds.ENTRY_DELETE.name())) {
                return Flowable.error(new IOException("file has been deleted"));
            }
            // we hope that ENTRY_CREATE and ENTRY_DELETE events never get wrapped up into
            // ENTRY_OVERFLOW!
        }
        long length = file.length();
        if (length > state.position) {
            // apply using method to ensure fis is closed on
            // termination or unsubscription
            return Flowable.using( //
                    () -> new FileInputStream(file), //
                    fis -> {
                        fis.skip(state.position);
                        return Bytes.from(fis, chunkSize) //
                                .doOnNext(x -> state.position += x.length);
                    }, //
                    fis -> fis.close(), //
                    true);
        } else {
            return Flowable.empty();
        }
    }

    private final static Path getBasePath(final File file) {
        final Path path;
        if (file.exists() && file.isDirectory())
            path = Paths.get(file.toURI());
        else
            path = Paths.get(file.getParentFile().toURI());
        return path;
    }

}
