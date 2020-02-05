package com.lmax.disruptor.example;


import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.util.DaemonThreadFactory;

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/***
 *@className ThreeToOneDisruptor
 *
 *@description 参考这个类
 * {@link Pipeliner}
 *
 *@author <a href="http://youngitman.tech">青年IT男</a>
 *
 *@date 11:22 2020-02-05
 *
 *@JunitTest: {@link  }
 *
 *@version v1.0.0
 *
**/
public class ThreeToOneDisruptor {
    public static class DataEvent {
        Integer input;
        Integer[] output;

        public DataEvent(int size) {
            output = new Integer[size];
        }

        public static final EventFactory<DataEvent> FACTORY = () -> new DataEvent(3);
    }

    public static class TransformingHandler implements EventHandler<DataEvent> {
        private final int outputIndex;

        public TransformingHandler(int outputIndex) {
            this.outputIndex = outputIndex;
        }

        @Override
        public void onEvent(DataEvent event, long sequence, boolean endOfBatch) throws Exception {
            // Do Stuff.
            event.output[outputIndex] = doSomething(event.input);
        }

        private Integer doSomething(Integer input) {
            // Do required transformation here....
            System.out.println("The thread name is "+Thread.currentThread().getName()+" for handed data is "+input);
            return input;
        }
    }

    /***
     *@className ThreeToOneDisruptor
     *
     *@description 聚合处理
     *
     *@author <a href="http://youngitman.tech">青年IT男</a>
     *
     *@date 11:22 2020-02-05
     *
     *@JunitTest: {@link  }
     *
     *@version v1.0.0
     *
    **/
    public static class CollatingHandler implements EventHandler<DataEvent> {
        @Override
        public void onEvent(DataEvent event, long sequence, boolean endOfBatch) throws Exception {
            collate(event.output);
        }

        private void collate(Integer[] output) {
            // Do required collation here....
//            Arrays.stream(output).forEach((d)-> System.out.println(d));
            System.out.println("data start");
            Optional.ofNullable(output).ifPresent((d)->  Arrays.stream(output).forEach((d1)-> System.out.println(d1)));
            System.out.println("data end");
        }
    }

    public static void main(String[] args) throws InterruptedException {
        Executor executor = Executors.newFixedThreadPool(4);
        Disruptor<DataEvent> disruptor = new Disruptor<DataEvent>(
                DataEvent.FACTORY, 1024, DaemonThreadFactory.INSTANCE);

        TransformingHandler handler1 = new TransformingHandler(0);
        TransformingHandler handler2 = new TransformingHandler(1);
        TransformingHandler handler3 = new TransformingHandler(2);
        CollatingHandler collator = new CollatingHandler();

        disruptor.handleEventsWith(handler1, handler2, handler3).then(collator);

        RingBuffer<DataEvent> ringBuffer = disruptor.start();

        for (int i = 0; i < 5; i++) {
            //发布数据
            long sequence = ringBuffer.next();

            DataEvent event = ringBuffer.get(sequence);
            event.input = i;

            ringBuffer.publish(sequence);
        }

        TimeUnit.MILLISECONDS.sleep(100_000_000);
    }
}
