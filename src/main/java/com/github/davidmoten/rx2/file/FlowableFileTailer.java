package com.github.davidmoten.rx2.file;

import java.io.File;

import org.reactivestreams.Subscriber;

import io.reactivex.Flowable;

public class FlowableFileTailer extends Flowable<byte[]> {

    private Flowable<?> events;
    private File file;
    private long startPosition;
    private int chunkSize;

    public FlowableFileTailer(Flowable<?> events, File file, long startPosition, int chunkSize) {
        this.events = events;
        this.file = file;
        this.startPosition = startPosition;
        this.chunkSize = chunkSize;
        // TODO Auto-generated constructor stub
    }

    @Override
    protected void subscribeActual(Subscriber<? super byte[]> s) {
        // TODO Auto-generated method stub
        
    }

}
