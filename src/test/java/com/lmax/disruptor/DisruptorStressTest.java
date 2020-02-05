package com.lmax.disruptor;

import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.LockSupport;

import static java.lang.Math.max;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

/***
 *@className DisruptorStressTest
 *
 *@description 多线程发布数据，多线程消费数据
 *
 *@author <a href="http://youngitman.tech">青年IT男</a>
 *
 *@date 22:26 2020-02-05
 *
 *@JunitTest: {@link  }
 *
 *@version v1.0.0
 *
**/
public class DisruptorStressTest {
    private final ExecutorService executor = Executors.newCachedThreadPool();

    @Test
    public void shouldHandleLotsOfThreads() throws Exception {
        Disruptor<TestEvent> disruptor = new Disruptor<>(
                TestEvent.FACTORY, 1 << 16, DaemonThreadFactory.INSTANCE,
                ProducerType.MULTI, new BusySpinWaitStrategy());
        RingBuffer<TestEvent> ringBuffer = disruptor.getRingBuffer();
        disruptor.setDefaultExceptionHandler(new FatalExceptionHandler());

        int threads = max(1, Runtime.getRuntime().availableProcessors() / 2);

        int iterations = 200000;
        int publisherCount = threads;
        int handlerCount = threads;

        CyclicBarrier barrier = new CyclicBarrier(publisherCount);
        CountDownLatch latch = new CountDownLatch(publisherCount);

        //初始化EventHandler
        TestEventHandler[] handlers = initialise(disruptor, new TestEventHandler[handlerCount]);
        //初始化事件发布者
        Publisher[] publishers = initialise(new Publisher[publisherCount], ringBuffer, iterations, barrier, latch);

        disruptor.start();

        //交给线程池执行run方法
        for (Publisher publisher : publishers) {
            executor.execute(publisher);
        }

        //等待所有生产者执行完成
        latch.await();
        //等待消费者消费完所有数据
        while (ringBuffer.getCursor() < (iterations - 1)) {
            LockSupport.parkNanos(1);
        }

        disruptor.shutdown();

        for (Publisher publisher : publishers) {
            assertThat(publisher.failed, is(false));
        }

        for (TestEventHandler handler : handlers) {
            assertThat(handler.messagesSeen, is(not(0)));
            assertThat(handler.failureCount, is(0));
        }
    }

    private Publisher[] initialise(
            Publisher[] publishers, RingBuffer<TestEvent> buffer,
            int messageCount, CyclicBarrier barrier, CountDownLatch latch) {
        for (int i = 0; i < publishers.length; i++) {
            publishers[i] = new Publisher(buffer, messageCount, barrier, latch);
        }

        return publishers;
    }

    @SuppressWarnings("unchecked")
    private TestEventHandler[] initialise(Disruptor<TestEvent> disruptor, TestEventHandler[] testEventHandlers) {
        for (int i = 0; i < testEventHandlers.length; i++) {
            TestEventHandler handler = new TestEventHandler();
            //并行处理等价于disruptor.handleEventsWith(handler1,handler2,handler3)
            //和disruptor.handleEventsWith(handler1).handleEventsWith(handler2).then(handler3)不同，后面这种是串行处理
            disruptor.handleEventsWith(handler);
            testEventHandlers[i] = handler;
        }

        return testEventHandlers;
    }

    private static class TestEventHandler implements EventHandler<TestEvent> {
        public int failureCount = 0;
        public int messagesSeen = 0;

        TestEventHandler() {
        }

        @Override
        public void onEvent(TestEvent event, long sequence, boolean endOfBatch) throws Exception {
            if (event.sequence != sequence ||
                    event.a != sequence + 13 ||
                    event.b != sequence - 7 ||
                    !("wibble-" + sequence).equals(event.s)) {
                failureCount++;
            }

            messagesSeen++;
        }
    }

    private static class Publisher implements Runnable {
        private final RingBuffer<TestEvent> ringBuffer;
        private final CyclicBarrier barrier;
        private final int iterations;
        private final CountDownLatch shutdownLatch;

        public boolean failed = false;

        Publisher(
                RingBuffer<TestEvent> ringBuffer,
                int iterations,
                CyclicBarrier barrier,
                CountDownLatch shutdownLatch) {
            this.ringBuffer = ringBuffer;
            this.barrier = barrier;
            this.iterations = iterations;
            this.shutdownLatch = shutdownLatch;
        }

        @Override
        public void run() {
            try {
                //等待所有生产者准备完成
                barrier.await();

                int i = iterations;
                //循环发送数据
                while (--i != -1) {
                    long next = ringBuffer.next();
                    TestEvent testEvent = ringBuffer.get(next);
                    testEvent.sequence = next;
                    testEvent.a = next + 13;
                    testEvent.b = next - 7;
                    testEvent.s = "wibble-" + next;
                    ringBuffer.publish(next);
                }
            } catch (Exception e) {
                failed = true;
            } finally {
                //生产者执行完成计数减1
                shutdownLatch.countDown();
            }
        }
    }

    private static class TestEvent {
        public long sequence;
        public long a;
        public long b;
        public String s;

        public static final EventFactory<TestEvent> FACTORY = () -> new TestEvent();
    }
}
