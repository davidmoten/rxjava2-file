rxjava-file
===========

Status: *released to Maven Central*

Requires Java 7.

Observable utilities for files:
* tail a file (either lines or byte[]) 
* trigger tail updates using Java 7 and later NIO ```WatchService``` events
* or trigger tail updates using any Observable
* stream ```WatchEvent```s from a ```WatchService```

[Release notes](http://davidmoten.github.io/rxjava-file/RELEASE_NOTES.md)

Continuous integration with Jenkins: <a href="https://xuml-tools.ci.cloudbees.com/"><img src="https://xuml-tools.ci.cloudbees.com/job/rxjava-file/badge/icon"/></a>

Maven site reports are [here](http://davidmoten.github.io/rxjava-file/index.html) including [javadoc](http://davidmoten.github.io/rxjava-file/apidocs/index.html).

Getting started
----------------
Add this maven dependency to your pom.xml:
```xml
<dependency>
  <groupId>com.github.davidmoten</groupId>
  <artifactId>rxjava-file</artifactId>
  <version>0.1</version>
</dependency>
```

How to build
----------------

```bash
git clone https://github.com/davidmoten/rxjava-file
cd rxjava-file
maven clean install 
```

Examples
--------------

###Tail a text file with NIO

Tail the lines of the text log file ```/var/log/server.log``` as an ```Observable<String>```:

```java
import com.github.davidmoten.rx.FileObservable;
import rx.Observable;
import java.io.File; 
 
Observable<String> items = 
     FileObservable.tailer()
                   .file("var/log/server.log")
                   .startPosition(0)
                   .sampleTimeMs(500)
                   .chunkSize(8192)
                   .utf8()
                   .tailText();
                     
```
or, using defaults (will use default charset):
```java
import com.github.davidmoten.rx.FileObservable;
import rx.Observable;
import java.io.File; 
 
Observable<String> items = 
     FileObservable.tailer()
                   .file("var/log/server.log")
                   .tailText();
```
###Tail a text file without NIO

The above example uses a ```WatchService``` to generate ```WatchEvent```s to prompt rereads of the end of the file to perform the tail.

To use polling instead (say every 5 seconds):

```java
Observable<String> items = 
                   .tailer()
                   .file(new File("var/log/server.log"))
                   .startPosition(0)
                   .sampleTimeMs(500)
                   .chunkSize(8192)
                   .utf8()
                   .source(Observable.interval(5, TimeUnit.SECONDS)
                   .tailText();
```


