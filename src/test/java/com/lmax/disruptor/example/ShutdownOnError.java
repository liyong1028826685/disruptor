package com.lmax.disruptor.example;

import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

/***
 *@className ShutdownOnError
 *
 *@description 自定义异常处理器
 *
 *@author <a href="http://youngitman.tech">青年IT男</a>
 *
 *@date 10:57 2020-02-05
 *
 *@JunitTest: {@link  }
 *
 *@version v1.0.0
 *
**/
public class ShutdownOnError {
    /**
     * DDD模式
     */
    private static class Event {
        public long value;

        public static final EventFactory<Event> FACTORY = () -> new Event();
    }

    private static class DefaultThreadFactory implements ThreadFactory {
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r);
        }
    }

    private static class Handler implements EventHandler<Event> {
        @Override
        public void onEvent(Event event, long sequence, boolean endOfBatch) throws Exception {
            // do work, if a failure occurs throw exception.
            if (sequence % 2 == 0) {
                throw new RuntimeException("偶数异常");
            }
            System.out.println("The sequence is "+sequence);
        }
    }

    /**
     *
     * 在这里异常被回掉{@link BatchEventProcessor#processEvents()} {@link BatchEventProcessor#notifyTimeout(long)}
     */
    private static final class ErrorHandler implements ExceptionHandler<Event> {
        private final AtomicBoolean running;

        private ErrorHandler(AtomicBoolean running) {
            this.running = running;
        }

        @Override
        public void handleEventException(Throwable ex, long sequence, Event event) {
            if (execeptionIsFatal(ex)) {
                //一般建议不要直接抛出异常
                throw new RuntimeException(ex);
            }
        }

        private boolean execeptionIsFatal(Throwable ex) {
            // Do what is appropriate here.
            return true;
        }

        @Override
        public void handleOnStartException(Throwable ex) {

        }
        /**
         *
         * Shutdown事件回掉{@link com.lmax.disruptor.BatchEventProcessor#notifyShutdown}
         *
         * @author liyong
         * @date 10:48 2020-02-05
         * @param ex
         * @exception
         * @return void
         **/
        @Override
        public void handleOnShutdownException(Throwable ex) {

        }
    }

    public static void main(String[] args) {
        Disruptor<Event> disruptor = new Disruptor<>(Event.FACTORY, 1024, new DefaultThreadFactory());

        AtomicBoolean running = new AtomicBoolean(true);

        ErrorHandler errorHandler = new ErrorHandler(running);

        final Handler handler = new Handler();
        disruptor.handleEventsWith(handler);
        disruptor.handleExceptionsFor(handler).with(errorHandler);

        disruptor.start();

        simplePublish(disruptor, running);


    }

    private static void simplePublish(Disruptor<Event> disruptor, AtomicBoolean running) {
        while (running.get()) {
            disruptor.publishEvent((event, sequence) -> event.value = sequence);
        }
    }

    private static void smarterPublish(Disruptor<Event> disruptor, AtomicBoolean running) {
        final RingBuffer<Event> ringBuffer = disruptor.getRingBuffer();

        boolean publishOk;
        do {
            publishOk = ringBuffer.tryPublishEvent((event, sequence) -> event.value = sequence);
        }
        while (publishOk && running.get());
    }
}
