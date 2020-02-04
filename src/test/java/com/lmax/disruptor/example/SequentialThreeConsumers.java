package com.lmax.disruptor.example;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.support.LongEvent;
import com.lmax.disruptor.util.DaemonThreadFactory;

import java.util.concurrent.TimeUnit;

/***
 *
 * 事件传递数据
 *
 * @author liyong
 * @date 17:35 2020-02-03
 *  * @param null
 * @exception
 * @return
 **/
public class SequentialThreeConsumers {
    private static class MyEvent {
        private Object a;
        private Object b;
        private Object c;
        private Object d;

        @Override
        public String toString() {
            return a+":"+b+":"+c+":"+d;
        }
    }

    private static EventFactory<MyEvent> factory = new EventFactory<MyEvent>() {
        @Override
        public MyEvent newInstance() {
            return new MyEvent();
        }
    };

    private static EventHandler<MyEvent> handler1 = new EventHandler<MyEvent>() {
        @Override
        public void onEvent(MyEvent event, long sequence, boolean endOfBatch) throws Exception {
            event.b = event.a;
            System.out.println("The "+event+" Data is handled for the  handler1，and Thread numer is "+Thread.currentThread().getName());
        }
    };

    private static EventHandler<MyEvent> handler2 = new EventHandler<MyEvent>() {
        @Override
        public void onEvent(MyEvent event, long sequence, boolean endOfBatch) throws Exception {
            event.c = event.b;
            System.out.println("The "+event+" Data is handled for the  handler2，and Thread numer is "+Thread.currentThread().getName());
        }
    };

    private static EventHandler<MyEvent> handler3 = new EventHandler<MyEvent>() {
        @Override
        public void onEvent(MyEvent event, long sequence, boolean endOfBatch) throws Exception {
            event.d = event.c;
            System.out.println("The "+event+" Data is handled for the  handler3，and Thread numer is "+Thread.currentThread().getName());
        }
    };

    public static void main(String[] args) throws InterruptedException {
        Disruptor<MyEvent> disruptor = new Disruptor<MyEvent>(factory, 4, DaemonThreadFactory.INSTANCE);

        /**
         * The 10:10:null:null Data is handled for the  handler1，and Thread numer is Thread-1
         * The 10:10:10:null Data is handled for the  handler2，and Thread numer is Thread-2
         * The 10:10:10:10 Data is handled for the  handler3，and Thread numer is Thread-3
         * The 10:10:null:null Data is handled for the  handler1，and Thread numer is Thread-1
         * The 10:10:10:null Data is handled for the  handler2，and Thread numer is Thread-2
         * The 10:10:10:10 Data is handled for the  handler3，and Thread numer is Thread-3
         * The 10:10:null:null Data is handled for the  handler1，and Thread numer is Thread-1
         * The 10:10:10:null Data is handled for the  handler2，and Thread numer is Thread-2
         * The 10:10:10:10 Data is handled for the  handler3，and Thread numer is Thread-3
         * The 10:10:null:null Data is handled for the  handler1，and Thread numer is Thread-1
         * The 10:10:10:null Data is handled for the  handler2，and Thread numer is Thread-2
         * The 10:10:10:10 Data is handled for the  handler3，and Thread numer is Thread-3
         */
        //handler1->handler2->handler3，必须依赖顺序处理 ，使用不同的SequenceBarrier，使用dependentSequence进行依赖处理，不同EventHandlerGroup级别
        //disruptor.handleEventsWith(handler1).then(handler2).then(handler3);

        //同上
        //disruptor.handleEventsWith(handler1).handleEventsWith(handler2).handleEventsWith(handler3);

        //并行，使用相同的SequenceBarrier，不依赖dependentSequence进行依赖处理，同一个EventHandlerGroup级别
        disruptor.handleEventsWith(handler1,handler2,handler3);


        //启动
        RingBuffer<MyEvent> ringBuffer = disruptor.start();

        for(int i=0;i<5;i++){
            //发布数据
            long sequence = ringBuffer.next();

            MyEvent event = ringBuffer.get(sequence);
            event.a = 10;

            ringBuffer.publish(sequence);
        }

        //防止JVM退出
        TimeUnit.MILLISECONDS.sleep(1000_000);
    }
}
