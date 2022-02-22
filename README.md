# rxjava2-file

<a href="https://github.com/davidmoten/rxjava2-file/actions/workflows/ci.yml"><img src="https://github.com/davidmoten/rxjava2-file/actions/workflows/ci.yml/badge.svg"/></a><br/>
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.davidmoten/rxjava2-file/badge.svg?style=flat)](https://maven-badges.herokuapp.com/maven-central/com.github.davidmoten/rxjava2-file)<br/>
[![codecov](https://codecov.io/gh/davidmoten/rxjava2-file/branch/master/graph/badge.svg)](https://codecov.io/gh/davidmoten/rxjava2-file)<br/>


Status: *released to Maven Central*

Requires Java 8+.

Flowable utilities for files:
* tail a file (either lines or byte[]) 
* trigger tail updates using Java 8 and later NIO ```WatchService``` events
* or trigger tail updates using any Flowable
* stream ```WatchEvent```s from a ```WatchService```
* tested on Linux, OSX ([notes](#osx)), Windows ([notes](#windows))
* Note that `WatchService` is problematic on OSX and Windows (see notes below) so your best bet is Linux!

Maven site reports are [here](https://davidmoten.github.io/rxjava2-file) including [javadoc](https://davidmoten.github.io/rxjava2-file/apidocs/index.html).

## Getting started
Add this maven dependency to your pom.xml:
```xml
<dependency>
  <groupId>com.github.davidmoten</groupId>
  <artifactId>rxjava2-file</artifactId>
  <version>VERSION_HERE</version>
</dependency>
```

## How to build

```bash
git clone https://github.com/davidmoten/rxjava2-file
cd rxjava2-file
mvn clean install 
```

## Examples

### Tail a text file with NIO

Tail the lines of the text log file ```/var/log/server.log``` as a ```Flowable<String>```:

```java
import com.github.davidmoten.rx2.file.Files;

Flowable<String> lines = 
     Files.tailLines("/var/log/server.log")
          .nonBlocking()
          .pollingInterval(500, TimeUnit.MILLISECONDS, Schedulers.io())
          // set a private sun modifier that improves OSX responsiveness
          .modifier(SensitivityWatchEventModifier.HIGH)
          .startPosition(0)
          .chunkSize(8192)
          .utf8()
          .build();
```
or, using defaults of startPosition 0, chunkSize 8192, charset UTF-8, scheduler `Schedulers.io()`:
```java
Flowable<String> items = 
     Files.tailLines("/var/log/server.log").nonBlocking().build();
	  
```
### Tail a text file without NIO

The above example uses a ```WatchService``` to generate ```WatchEvent```s to prompt rereads of the end of the file to perform the tail.

To use polling without a `WatchService` (say every 5 seconds):

```java
Flowable<String> items = 
  Files.tailLines("/var/log/server.log")
       .events(Flowable.interval(5, TimeUnit.SECONDS))
       .build();
```

### Tail a binary file with NIO
```java
Flowable<byte[]> items = 
  Files.tailBytes("/tmp/dump.bin").blocking().build();
```

### Tail a binary file without NIO
```java
Flowable<byte[]> items = 
  Files.tailBytes("/tmp/dump.bin")
       .events(Flowable.interval(5, TimeUnit.SECONDS))
       .build();
```

### Stream WatchService events for a file
```java
Flowable<WatchEvent<?>> events = 
  Files
    .watch(file)
    .nonBlocking()
    .scheduler(Schedulers.io())
    .pollInterval(1, TimeUnit.MINUTES)
    .build();
```
## Backpressure
When `tailLines` or `tailBytes` is used a conversion to `Flowable` occurs on the `WatchEvent` stream. This is desirable to handle large amounts of data being tailed in combination with a slow processor (e.g. a network call). The default strategy is BUFFER but the strategy is specifiable in the `tailLines` and `tailBytes` builders.
 
## Non-blocking and blocking
Two alternatives are supported by the library for getting file change events from a `WatchService`. The `nonBlocking()` builder methods configure the stream to use events via `WatchService.poll` which is a non-blocking call (but may involve some I/O?). The `blocking()` builder methods configure the stream to use events via `WatchService.take` which is a blocking call.

So when specify `nonBlocking()` you end up with a stream that is asynchronous and `blocking()` gives you a synchronous stream (everything happens on the current thread unless of course you add asynchrony to the returned `Flowable`).

## OSX
Apparently the `WatchService` can be slow on OSX (see [here](https://stackoverflow.com/questions/9588737/is-java-7-watchservice-slow-for-anyone-else)). Note that the first example above shows how to pass a special `WatchEvent.Modifier` which some find has a beneficial effect. Without that the `WatchService` can take >10 seconds to detect changes to the file system.

## Windows
Detecting changes to files on Windows also seems problematic. See https://stackoverflow.com/questions/24306875/watchservice-in-windows-7-does-not-work.

