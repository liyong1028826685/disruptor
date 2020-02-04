package com.lmax.disruptor.example;

import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.support.StubEvent;
import com.lmax.disruptor.util.DaemonThreadFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DynamiclyAddHandler {
    private static class DynamicHandler implements EventHandler<StubEvent>, LifecycleAware {
        private final CountDownLatch shutdownLatch = new CountDownLatch(1);

        @Override
        public void onEvent(final StubEvent event, final long sequence, final boolean endOfBatch) throws Exception {
        }

        @Override
        public void onStart() {

        }

        /***
         *
         * 参考{@link BatchEventProcessor#run()}
         *
         * @author liyong
         * @date 17:15 2020-02-04
         * @param
         * @exception
         * @return void
         **/
        @Override
        public void onShutdown() {
            //当处理器完成是回掉
            shutdownLatch.countDown();
        }

        /***
         *
         * 阻塞当前线程
         *
         * @author liyong
         * @date 17:16 2020-02-04
         * @param
         * @exception
         * @return void
         **/
        public void awaitShutdown() throws InterruptedException {
            shutdownLatch.await();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        ExecutorService executor = Executors.newCachedThreadPool(DaemonThreadFactory.INSTANCE);

        // Build a disruptor and start it.
        Disruptor<StubEvent> disruptor = new Disruptor<StubEvent>(
                StubEvent.EVENT_FACTORY, 1024, DaemonThreadFactory.INSTANCE);
        RingBuffer<StubEvent> ringBuffer = disruptor.start();

        // 消费事件处理器1
        DynamicHandler handler1 = new DynamicHandler();
        //关联事件处理器到BatchEventProcessor
        BatchEventProcessor<StubEvent> processor1 =
                new BatchEventProcessor<StubEvent>(ringBuffer, ringBuffer.newBarrier(), handler1);

        // 消费事件处理器2
        DynamicHandler handler2 = new DynamicHandler();
        //关联事件处理器到BatchEventProcessor
        BatchEventProcessor<StubEvent> processor2 =
                new BatchEventProcessor<StubEvent>(ringBuffer, ringBuffer.newBarrier(processor1.getSequence()), handler2);

        //添加两个处理器到Sequence到MultiProducerSequencer
        ringBuffer.addGatingSequences(processor1.getSequence(), processor2.getSequence());

        // 添加两个处理器BatchEventProcessor实现Runnable接口，可以丢线程池
        executor.execute(processor1);
        executor.execute(processor2);

        // Remove a processor.

        //停止处理器消费消息
        processor2.halt();
        // 阻塞当前线程等待所有处理器完成
        handler2.awaitShutdown();
        // Remove the gating sequence from the ring buffer
        ringBuffer.removeGatingSequence(processor2.getSequence());
    }
}
