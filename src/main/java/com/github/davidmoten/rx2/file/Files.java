
package com.github.davidmoten.rx2.file;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.ClosedWatchServiceException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchEvent.Modifier;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

import com.github.davidmoten.guavamini.Lists;
import com.github.davidmoten.guavamini.Preconditions;
import com.github.davidmoten.rx2.Bytes;

import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.Observable;
import io.reactivex.Scheduler;
import io.reactivex.functions.Function;
import io.reactivex.functions.Predicate;
import io.reactivex.observables.GroupedObservable;
import io.reactivex.schedulers.Schedulers;

/**
 * Flowable utility methods related to {@link File}.
 */
public final class Files {

    private static final int DEFAULT_POLLING_INTERVAL_MS = 1000;
    public static final int DEFAULT_MAX_BYTES_PER_EMISSION = 8192;
    public static final List<Kind<?>> ALL_KINDS = Lists.newArrayList(StandardWatchEventKinds.ENTRY_CREATE,
            StandardWatchEventKinds.ENTRY_DELETE, StandardWatchEventKinds.ENTRY_MODIFY,
            StandardWatchEventKinds.OVERFLOW);

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
    private static Flowable<byte[]> tailBytes(File file, long startPosition, long sampleTimeMs, int chunkSize,
            Observable<?> events) {
        Preconditions.checkNotNull(file);
        return eventsToBytes(sampleModifyOrOverflowEventsOnly(events, sampleTimeMs), //
                file, startPosition, chunkSize);
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
            Observable<?> events) {
        Preconditions.checkNotNull(file);
        Preconditions.checkNotNull(charset);
        Preconditions.checkNotNull(events);
        return toLines(eventsToBytes(events, file, startPosition, chunkSize), charset);
    }

    private static Observable<WatchEvent<?>> events(WatchService watchService, Scheduler scheduler, long intervalMs) {
        Preconditions.checkNotNull(watchService, "watchService cannot be null");
        Preconditions.checkNotNull(scheduler, "scheduler cannot be null");
        Preconditions.checkArgument(intervalMs > 0, "intervalMs must be positive");
        return Observable.interval(intervalMs, TimeUnit.MILLISECONDS, scheduler) //
                .flatMap(x -> {
                    try {
                        WatchKey key = watchService.poll();
                        if (key != null && key.isValid()) {
                            Observable<WatchEvent<?>> r = Observable.fromIterable(key.pollEvents());
                            key.reset();
                            return r;
                        } else {
                            return Observable.empty();
                        }
                    } catch (ClosedWatchServiceException e) {
                        // ignore
                        return Observable.empty();
                    }
                });
    }

    private static Observable<WatchEvent<?>> eventsBlocking(WatchService watchService) {
        Preconditions.checkNotNull(watchService, "watchService cannot be null");
        return Observable.<WatchEvent<?>, Queue<WatchEvent<?>>>generate(() -> new LinkedList<WatchEvent<?>>(), //
                (q, emitter) -> {
                    try {
                        while (q.isEmpty()) {
                            // blocking call
                            WatchKey key = watchService.take();
                            if (key.isValid()) {
                                q.addAll(key.pollEvents());
                            }
                            key.reset();
                        }
                        emitter.onNext(q.poll());
                    } catch (ClosedWatchServiceException e) {
                        // ignore
                        emitter.onComplete();
                    } catch (Throwable e) {
                        emitter.onError(e);
                    }
                }, q -> q.clear());

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
    private static Observable<WatchEvent<?>> events(File file, Scheduler scheduler, long pollingIntervalMs,
            List<Kind<?>> kinds, List<Modifier> modifiers) {
        return Observable.using(() -> watchService(file, kinds, modifiers), //
                ws -> events(ws, scheduler, pollingIntervalMs)
                        // restrict to events related to the file
                        .filter(onlyRelatedTo(file)), //
                ws -> ws.close(), true);
    }

    private static Observable<WatchEvent<?>> eventsBlocking(File file, List<Kind<?>> kinds, List<Modifier> modifiers) {
        return Observable.using(() -> watchService(file, kinds, modifiers), //
                ws -> eventsBlocking(ws)
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
    private static WatchService watchService(File file, List<Kind<?>> kinds, List<Modifier> modifiers)
            throws IOException {
        final Path path = getBasePath(file);
        WatchService watchService = path.getFileSystem().newWatchService();
        path.register(watchService, kinds.toArray(new Kind<?>[] {}), modifiers.toArray(new Modifier[] {}));
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

    private static Observable<Object> sampleModifyOrOverflowEventsOnly(Observable<?> events, final long sampleTimeMs) {
        return events
                // group by true if is modify or overflow, false otherwise
                .groupBy(IS_MODIFY_OR_OVERFLOW)
                // only sample if is modify or overflow
                .flatMap(sampleIfTrue(sampleTimeMs));
    }

    private static Function<GroupedObservable<Boolean, ?>, Observable<?>> sampleIfTrue(final long sampleTimeMs) {
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

        WatchEventsBuilder(File file) {
            this.file = file;
        }

        /**
         * Uses {@link WatchService#poll} under the covers.
         * @return builder
         */
        public WatchEventsNonBlockingBuilder nonBlocking() {
            return new WatchEventsNonBlockingBuilder(file);
        }

        /**
         * Uses blocking {@link WatchService#take} under the covers.
         * @return builder
         */
        public WatchEventsBlockingBuilder blocking() {
            return new WatchEventsBlockingBuilder(file);
        }

    }

    public static final class WatchEventsNonBlockingBuilder {
        private final File file;
        private Optional<Scheduler> scheduler = Optional.empty();
        private long pollInterval = DEFAULT_POLLING_INTERVAL_MS;
        private TimeUnit pollIntervalUnit = TimeUnit.MILLISECONDS;
        private final List<Kind<?>> kinds = new ArrayList<>();
        private final List<Modifier> modifiers = new ArrayList<>();

        private WatchEventsNonBlockingBuilder(File file) {
            Preconditions.checkNotNull(file, "file cannot be null");
            this.file = file;
        }

        public WatchEventsNonBlockingBuilder pollInterval(long interval, TimeUnit unit, Scheduler scheduler) {
            Preconditions.checkNotNull(unit);
            Preconditions.checkNotNull(scheduler);
            this.pollInterval = interval;
            this.pollIntervalUnit = unit;
            this.scheduler = Optional.ofNullable(scheduler);
            return this;
        }

        public WatchEventsNonBlockingBuilder pollInterval(long interval, TimeUnit unit) {
            return pollInterval(interval, unit, Schedulers.io());
        }

        /**
         * If no kind is specified then all {@link StandardWatchEventKinds} are used.
         * 
         * @param kind
         *            kind to add
         * @return this
         */
        public WatchEventsNonBlockingBuilder kind(Kind<?> kind) {
            Preconditions.checkNotNull(kind);
            this.kinds.add(kind);
            return this;
        }

        public WatchEventsNonBlockingBuilder modifier(Modifier modifier) {
            Preconditions.checkNotNull(modifier);
            this.modifiers.add(modifier);
            return this;
        }

        /**
         * If no kind is specified then all {@link StandardWatchEventKinds} are used.
         * 
         * @param kinds
         *            kinds to add
         * @return this
         */
        public WatchEventsNonBlockingBuilder kinds(Kind<?>... kinds) {
            Preconditions.checkNotNull(kinds);
            for (Kind<?> kind : kinds) {
                this.kinds.add(kind);
            }
            return this;
        }

        public Observable<WatchEvent<?>> build() {
            List<Kind<?>> kindsCopy = new ArrayList<>(kinds);
            if (kindsCopy.isEmpty()) {
                kindsCopy.add(StandardWatchEventKinds.ENTRY_CREATE);
                kindsCopy.add(StandardWatchEventKinds.ENTRY_DELETE);
                kindsCopy.add(StandardWatchEventKinds.ENTRY_MODIFY);
                kindsCopy.add(StandardWatchEventKinds.OVERFLOW);
            }
            return Observable.using( //
                    () -> watchService(file, kindsCopy, modifiers), //
                    ws -> Files.events(ws, scheduler.orElse(Schedulers.io()), pollIntervalUnit.toMillis(pollInterval)), //
                    ws -> ws.close(), //
                    true);
        }

    }

    public static final class WatchEventsBlockingBuilder {
        private final File file;
        private final List<Kind<?>> kinds = new ArrayList<>();
        private final List<Modifier> modifiers = new ArrayList<>();

        private WatchEventsBlockingBuilder(File file) {
            Preconditions.checkNotNull(file);
            this.file = file;
        }

        /**
         * If no kind is specified then all {@link StandardWatchEventKinds} are used.
         * 
         * @param kind
         *            kind to add
         * @return this
         */
        public WatchEventsBlockingBuilder kind(Kind<?> kind) {
            Preconditions.checkNotNull(kind);
            this.kinds.add(kind);
            return this;
        }

        public WatchEventsBlockingBuilder modifier(Modifier modifier) {
            Preconditions.checkNotNull(modifier);
            this.modifiers.add(modifier);
            return this;
        }

        /**
         * If no kind is specified then all {@link StandardWatchEventKinds} are used.
         * 
         * @param kinds
         *            kinds to add
         * @return this
         */
        public WatchEventsBlockingBuilder kinds(Kind<?>... kinds) {
            Preconditions.checkNotNull(kinds);
            for (Kind<?> kind : kinds) {
                this.kinds.add(kind);
            }
            return this;
        }

        public Observable<WatchEvent<?>> build() {
            List<Kind<?>> kindsCopy = new ArrayList<>(kinds);
            if (kindsCopy.isEmpty()) {
                kindsCopy.add(StandardWatchEventKinds.ENTRY_CREATE);
                kindsCopy.add(StandardWatchEventKinds.ENTRY_DELETE);
                kindsCopy.add(StandardWatchEventKinds.ENTRY_MODIFY);
                kindsCopy.add(StandardWatchEventKinds.OVERFLOW);
            }
            return Observable.using( //
                    () -> watchService(file, kindsCopy, modifiers), //
                    ws -> Files.eventsBlocking(ws), //
                    ws -> ws.close(), //
                    true);
        }

    }

    public static TailBytesBuilder tailBytes(File file) {
        return new TailBytesBuilder(file);
    }

    public static TailBytesBuilder tailBytes(String filename) {
        return tailBytes(new File(filename));
    }

    public static TailLinesBuilder tailLines(File file) {
        return new TailLinesBuilder(file);
    }

    public static TailLinesBuilder tailLines(String filename) {
        return tailLines(new File(filename));
    }

    public static final class TailBytesBuilder {
        private final File file;

        TailBytesBuilder(File file) {
            this.file = file;
        }
        
        /**
         * Uses {@link WatchService#poll} under the covers.
         * @return builder
         */
        public TailBytesNonBlockingBuilder nonBlocking() {
            return new TailBytesNonBlockingBuilder(file);
        }
        
        /**
         * Uses blocking {@link WatchService#take} under the covers.
         * @return builder
         */
        public TailBytesBlockingBuilder blocking() {
            return new TailBytesBlockingBuilder(file);
        }
    }

    public static final class TailBytesNonBlockingBuilder {

        private final File file;
        private long startPosition = 0;
        private int chunkSize = 8192;
        private long pollingIntervalMs = DEFAULT_POLLING_INTERVAL_MS;
        private Scheduler scheduler = Schedulers.io();
        private Observable<?> events;
        private final List<Modifier> modifiers = new ArrayList<>();

        TailBytesNonBlockingBuilder(File file) {
            Preconditions.checkNotNull(file);
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
        public TailBytesNonBlockingBuilder startPosition(long startPosition) {
            this.startPosition = startPosition;
            return this;
        }

        public TailBytesNonBlockingBuilder pollingInterval(long pollingInterval, TimeUnit unit, Scheduler scheduler) {
            Preconditions.checkNotNull(unit);
            Preconditions.checkNotNull(scheduler);
            this.pollingIntervalMs = unit.toMillis(pollingInterval);
            this.scheduler = scheduler;
            return this;
        }

        public TailBytesNonBlockingBuilder pollingInterval(long pollingInterval, TimeUnit unit) {
            Preconditions.checkNotNull(unit);
            return pollingInterval(pollingInterval, unit, Schedulers.io());
        }

        /**
         * Emissions from the tailed file will be no bigger than this.
         * 
         * @param chunkSize
         *            chunk size in bytes
         * @return this
         */
        public TailBytesNonBlockingBuilder chunkSize(int chunkSize) {
            this.chunkSize = chunkSize;
            return this;
        }

        public TailBytesNonBlockingBuilder events(Observable<?> events) {
            Preconditions.checkNotNull(events);
            this.events = events;
            return this;
        }

        public TailBytesNonBlockingBuilder modifier(Modifier modifier) {
            Preconditions.checkNotNull(modifier);
            this.modifiers.add(modifier);
            return this;
        }

        private Observable<?> events() {
            if (events == null) {
                return Files.events(file, scheduler, pollingIntervalMs, ALL_KINDS, modifiers);
            } else {
                return events;
            }
        }

        public Flowable<byte[]> build() {
            return Files.tailBytes(file, startPosition, pollingIntervalMs * 2, chunkSize, events());
        }

    }

    public static final class TailBytesBlockingBuilder {

        private final File file;
        private long startPosition = 0;
        private int chunkSize = 8192;
        private Observable<?> events;
        private final List<Modifier> modifiers = new ArrayList<>();
        private long sampleTimeMs = 1000;

        TailBytesBlockingBuilder(File file) {
            Preconditions.checkNotNull(file);
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
        public TailBytesBlockingBuilder startPosition(long startPosition) {
            this.startPosition = startPosition;
            return this;
        }

        public TailBytesBlockingBuilder sampleTime(long time, TimeUnit unit) {
            Preconditions.checkNotNull(unit);
            this.sampleTimeMs = unit.toMillis(time);
            return this;
        }

        /**
         * Emissions from the tailed file will be no bigger than this.
         * 
         * @param chunkSize
         *            chunk size in bytes
         * @return this
         */
        public TailBytesBlockingBuilder chunkSize(int chunkSize) {
            this.chunkSize = chunkSize;
            return this;
        }

        public TailBytesBlockingBuilder events(Observable<?> events) {
            Preconditions.checkNotNull(events);
            this.events = events;
            return this;
        }

        public TailBytesBlockingBuilder modifier(Modifier modifier) {
            Preconditions.checkNotNull(modifier);
            this.modifiers.add(modifier);
            return this;
        }

        private Observable<?> events() {
            if (events == null) {
                return Files.eventsBlocking(file, ALL_KINDS, modifiers);
            } else {
                return events;
            }
        }

        public Flowable<byte[]> build() {
            return Files.tailBytes(file, startPosition, sampleTimeMs, chunkSize, events());
        }

    }
    
    public static final class TailLinesBuilder {
        
        private final File file;

        TailLinesBuilder(File file) {
            this.file  = file;
        }
        
        /**
         * Uses {@link WatchService#poll} under the covers.
         * @return builder
         */
        public TailLinesNonBlockingBuilder nonBlocking() {
            return new TailLinesNonBlockingBuilder(file);
        }
        
        /**
         * Uses blocking {@link WatchService#take} under the covers.
         * @return builder
         */
        public TailLinesBlockingBuilder blocking() {
            return new TailLinesBlockingBuilder(file);
        }
    }

    public static final class TailLinesNonBlockingBuilder {

        private final File file;
        private long startPosition = 0;
        private int chunkSize = 8192;
        private Charset charset = StandardCharsets.UTF_8;
        private long pollingIntervalMs = DEFAULT_POLLING_INTERVAL_MS;
        private Scheduler scheduler = Schedulers.io();
        private Observable<?> events;
        private final List<Modifier> modifiers = new ArrayList<>();

        TailLinesNonBlockingBuilder(File file) {
            Preconditions.checkNotNull(file);
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
        public TailLinesNonBlockingBuilder startPosition(long startPosition) {
            this.startPosition = startPosition;
            return this;
        }

        public TailLinesNonBlockingBuilder pollingInterval(long pollingInterval, TimeUnit unit, Scheduler scheduler) {
            Preconditions.checkNotNull(unit);
            Preconditions.checkNotNull(scheduler);
            this.pollingIntervalMs = unit.toMillis(pollingInterval);
            this.scheduler = scheduler;
            return this;
        }

        public TailLinesNonBlockingBuilder pollingInterval(long pollingInterval, TimeUnit unit) {
            Preconditions.checkNotNull(unit);
            return pollingInterval(pollingInterval, unit, Schedulers.io());
        }

        /**
         * Emissions from the tailed file will be no bigger than this.
         * 
         * @param chunkSize
         *            chunk size in bytes
         * @return this
         */
        public TailLinesNonBlockingBuilder chunkSize(int chunkSize) {
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
        public TailLinesNonBlockingBuilder charset(Charset charset) {
            Preconditions.checkNotNull(charset);
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
        public TailLinesNonBlockingBuilder charset(String charset) {
            Preconditions.checkNotNull(charset);
            return charset(Charset.forName(charset));
        }

        public TailLinesNonBlockingBuilder utf8() {
            return charset("UTF-8");
        }

        public TailLinesNonBlockingBuilder modifier(Modifier modifier) {
            Preconditions.checkNotNull(modifier);
            this.modifiers.add(modifier);
            return this;
        }

        public TailLinesNonBlockingBuilder events(Observable<?> events) {
            Preconditions.checkNotNull(events);
            this.events = events;
            return this;
        }

        private Observable<?> events() {
            if (events == null) {
                return Files.events(file, scheduler, pollingIntervalMs, ALL_KINDS, modifiers);
            } else {
                return events;
            }
        }

        public Flowable<String> build() {
            return Files.tailLines(file, startPosition, chunkSize, charset, events());
        }
    }

    public static final class TailLinesBlockingBuilder {

        private final File file;
        private long startPosition = 0;
        private int chunkSize = 8192;
        private Charset charset = StandardCharsets.UTF_8;
        private Observable<?> events;
        private final List<Modifier> modifiers = new ArrayList<>();

        TailLinesBlockingBuilder(File file) {
            Preconditions.checkNotNull(file);
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
        public TailLinesBlockingBuilder startPosition(long startPosition) {
            this.startPosition = startPosition;
            return this;
        }

        /**
         * Emissions from the tailed file will be no bigger than this.
         * 
         * @param chunkSize
         *            chunk size in bytes
         * @return this
         */
        public TailLinesBlockingBuilder chunkSize(int chunkSize) {
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
        public TailLinesBlockingBuilder charset(Charset charset) {
            Preconditions.checkNotNull(charset);
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
        public TailLinesBlockingBuilder charset(String charset) {
            Preconditions.checkNotNull(charset);
            return charset(Charset.forName(charset));
        }

        public TailLinesBlockingBuilder utf8() {
            return charset("UTF-8");
        }

        public TailLinesBlockingBuilder modifier(Modifier modifier) {
            Preconditions.checkNotNull(modifier);
            this.modifiers.add(modifier);
            return this;
        }

        public TailLinesBlockingBuilder events(Observable<?> events) {
            Preconditions.checkNotNull(events);
            this.events = events;
            return this;
        }

        private Observable<?> events() {
            if (events == null) {
                return Files.eventsBlocking(file, ALL_KINDS, modifiers);
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

    private static Flowable<byte[]> eventsToBytes(Observable<?> events, File file, long startPosition, int chunkSize) {
        return Flowable.defer(() -> {
            State state = new State();
            state.position = startPosition;
            // TODO allow user to specify BackpressureStrategy
            return events.toFlowable(BackpressureStrategy.BUFFER) //
                    .flatMap(event -> eventToBytes(event, file, state, chunkSize));
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
