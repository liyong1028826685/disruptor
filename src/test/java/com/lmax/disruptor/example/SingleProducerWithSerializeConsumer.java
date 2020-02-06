package com.lmax.disruptor.example;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;

import java.util.concurrent.TimeUnit;

/**
 * @author <a href="http://youngitman.tech">青年IT男</a>
 * @version v1.0.0
 * @className SingleProducerWithSerializeConsumer
 * @description
 * @date 2020-02-06 14:56
 * @JunitTest: {@link  }
 **/
public class SingleProducerWithSerializeConsumer {
    private static class MyEvent {
        private Object a;
        private Object b;
        private Object c;
        private Object d;

        @Override
        public String toString() {
            return a + ":" + b + ":" + c + ":" + d;
        }
    }

    private static EventFactory<MyEvent> factory = () -> new SingleProducerWithSerializeConsumer.MyEvent();

    private static EventHandler<SingleProducerWithSerializeConsumer.MyEvent> handler1 = (event, sequence, endOfBatch) -> {
        event.b = event.a;
        System.out.println("The " + event + " Data is handled for the  handler1，and Thread numer is " + Thread.currentThread().getName());
    };

    private static EventHandler<SingleProducerWithSerializeConsumer.MyEvent> handler2 = (event, sequence, endOfBatch) -> {
        event.c = event.b;
        System.out.println("The " + event + " Data is handled for the  handler2，and Thread numer is " + Thread.currentThread().getName());
    };

    private static EventHandler<SingleProducerWithSerializeConsumer.MyEvent> handler3 = (event, sequence, endOfBatch) -> {
        event.d = event.c;
        System.out.println("The " + event + " Data is handled for the  handler3，and Thread numer is " + Thread.currentThread().getName());
    };

    public static void main(String[] args) throws InterruptedException {

        serialize01();

        serialize02();

        //防止JVM退出
        TimeUnit.MILLISECONDS.sleep(1000_000);
    }

    private static void publish(Disruptor disruptor) {
        //启动
        RingBuffer<MyEvent> ringBuffer = disruptor.start();

        for (int i = 0; i < 20; i++) {
            //发布数据
            long sequence = ringBuffer.next();

            SingleProducerWithSerializeConsumer.MyEvent event = ringBuffer.get(sequence);
            event.a = 10;

            ringBuffer.publish(sequence);
        }
    }

    /***
     *
     * 单线程生产多个消费者线程串行处理
     *
     * @author liyong
     * @date 15:06 2020-02-06
     * @param
     * @exception
     * @return void
     **/
    private static void serialize01() {
        //新建一个Disruptor实例,单线程生产数据，WaitStrategy采用BlockingWaitStrategy
        Disruptor<SingleProducerWithSerializeConsumer.MyEvent> disruptor = new Disruptor<>(factory, 4, DaemonThreadFactory.INSTANCE, ProducerType.SINGLE, new BlockingWaitStrategy());

        disruptor.handleEventsWith(handler1).then(handler2).then(handler3);

        publish(disruptor);
    }

    /***
     *
     * 多线程生产多个消费者线程串行处理
     *
     * @author liyong
     * @date 15:06 2020-02-06
     * @param
     * @exception
     * @return void
     **/
    private static void serialize02() {
        //新建一个Disruptor实例,单线程生产数据，WaitStrategy采用BlockingWaitStrategy
        Disruptor<SingleProducerWithSerializeConsumer.MyEvent> disruptor = new Disruptor<>(factory, 4, DaemonThreadFactory.INSTANCE, ProducerType.MULTI, new BlockingWaitStrategy());

        disruptor.handleEventsWith(handler1).then(handler2).then(handler3);

        new Thread(()->{
            publish(disruptor);
        }).start();
    }
}
