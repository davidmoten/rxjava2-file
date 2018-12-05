# rxjava2-file

<a href="https://travis-ci.org/davidmoten/rxjava2-file"><img src="https://travis-ci.org/davidmoten/rxjava2-file.svg"/></a><br/>
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.davidmoten/rxjava2-file/badge.svg?style=flat)](https://maven-badges.herokuapp.com/maven-central/com.github.davidmoten/rxjava2-file)</br>
[![codecov](https://codecov.io/gh/davidmoten/rxjava2-file/branch/master/graph/badge.svg)](https://codecov.io/gh/davidmoten/rxjava2-file)


Status: *pre-alpha (migration work in progress)*

Requires Java 8+.

Flowable utilities for files:
* tail a file (either lines or byte[]) 
* trigger tail updates using Java 8 and later NIO ```WatchService``` events
* or trigger tail updates using any Observable
* stream ```WatchEvent```s from a ```WatchService```
* backpressure support
* tested on Linux, OSX (let me know if you have problems on Windows)

Maven site reports TODO 

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

Tail the lines of the text log file ```/var/log/server.log``` as an ```Observable<String>```:

```java
import com.github.davidmoten.rx2.file.Files;

Flowable<String> lines = 
     Files.tailLines("/var/log/server.log")
          .pollingInterval(500, TimeUnit.MILLISECONDS)
          .scheduler(Schedulers.io())
          .modifier(SensitivityWatchEventModifier.HIGH)
          .startPosition(0)
          .chunkSize(8192)
          .utf8()
          .build();
```
or, using defaults of startPosition 0, chunkSize 8192, charset UTF-8, scheduler `Schedulers.io()`:
```java
Observable<String> items = 
     Files.tailLines("/var/log/server.log").build();
	  
```
### Tail a text file without NIO

The above example uses a ```WatchService``` to generate ```WatchEvent```s to prompt rereads of the end of the file to perform the tail.

To use polling instead (say every 5 seconds):

```java
Observable<String> items = 
     FileObservable.tailer()
                   .file(new File("var/log/server.log"))
                   .events(Observable.interval(5, TimeUnit.SECONDS))
                   .tailText();
```

### Tail a binary file with NIO
```java
Observable<byte[]> items = 
     FileObservable.tailer()
                   .file("/tmp/dump.bin")
                   .tail();
```

### Tail a binary file without NIO
```java
Observable<byte[]> items = 
     FileObservable.tailer()
                   .file("/tmp/dump.bin")
                   .events(Observable.interval(5, TimeUnit.SECONDS))
                   .tail();
```

### Stream WatchService events for a file
```java
Flowable<WatchEvent<?>> events = 
  Files
    .events(file)
    .scheduler(Schedulers.io())
    .pollInterval(1, TimeUnit.MINUTES)
    .build();
```

## OSX
Apparently the WatchService can be slow on OSX (see [here](https://stackoverflow.com/questions/9588737/is-java-7-watchservice-slow-for-anyone-else)). Note that the first example above shows how to pass a special `WatchEvent.Modifier` which some find has a beneficial effect.
