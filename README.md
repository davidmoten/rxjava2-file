rxjava2-file
===========
<a href="https://travis-ci.org/davidmoten/rxjava2-file"><img src="https://travis-ci.org/davidmoten/rxjava-file.svg"/></a><br/>
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.davidmoten/rxjava2-file/badge.svg?style=flat)](https://maven-badges.herokuapp.com/maven-central/com.github.davidmoten/rxjava-file)

Status: *pre-alpha (migration work in progress)*

Requires Java 8.

Observable utilities for files:
* tail a file (either lines or byte[]) 
* trigger tail updates using Java 8 and later NIO ```WatchService``` events
* or trigger tail updates using any Observable
* stream ```WatchEvent```s from a ```WatchService```
* backpressure support
* tested on Linux and Windows 8 (not OSX, help appreciated!)

[Release Notes](RELEASE_NOTES.md)

Maven site reports are [here](http://davidmoten.github.io/rxjava2-file/index.html) including [javadoc](http://davidmoten.github.io/rxjava-file/apidocs/index.html).

Getting started
----------------
Add this maven dependency to your pom.xml:
```xml
<dependency>
  <groupId>com.github.davidmoten</groupId>
  <artifactId>rxjava2-file</artifactId>
  <version>VERSION_HERE</version>
</dependency>
```

How to build
----------------

```bash
git clone https://github.com/davidmoten/rxjava2-file
cd rxjava2-file
mvn clean install 
```

Examples
--------------

### Tail a text file with NIO

Tail the lines of the text log file ```/var/log/server.log``` as an ```Observable<String>```:

```java
import com.github.davidmoten.rx.FileObservable;
import rx.Observable;
import java.io.File; 
 
Observable<String> items = 
     FileObservable.tailer()
                   .file("/var/log/server.log")
                   .startPosition(0)
                   .sampleTimeMs(500)
                   .chunkSize(8192)
                   .utf8()
                   .tailText();
                     
```
or, using defaults (will use default charset):
```java
Observable<String> items = 
     FileObservable.tailer()
                   .file("/var/log/server.log")
                   .tailText();
```

Note that if you want the ```Observable<String>``` to be emitting line by line then wrap 
it with a call like ```StringObservable.split(observable, "\n")```. ```StringObservable``` is in the RxJava *rxjava-string* artifact.

### Tail a text file without NIO

The above example uses a ```WatchService``` to generate ```WatchEvent```s to prompt rereads of the end of the file to perform the tail.

To use polling instead (say every 5 seconds):

```java
Observable<String> items = 
     FileObservable.tailer()
                   .file(new File("var/log/server.log"))
                   .source(Observable.interval(5, TimeUnit.SECONDS)
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
                   .source(Observable.interval(5, TimeUnit.SECONDS)
                   .tail();
```


