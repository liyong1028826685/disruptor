package com.lmax.disruptor.example;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.util.DaemonThreadFactory;

import java.util.concurrent.TimeUnit;

/***
 *@className Pipeliner
 *
 *@description 多线程并发处理数据，然后汇总处理，参考我画的图
 *<a href="http://youngitman.tech/wp-content/uploads/2020/02/Pipelier_Disruptor.png">处理流程图</a>
 *
 *@author <a href="http://youngitman.tech">青年IT男</a>
 *
 *@date 22:43 2020-02-04
 *
 *@JunitTest: {@link  }
 *
 *@version v1.0.0
 *
 **/
public class Pipeliner {
    public static void main(String[] args) throws InterruptedException {
        //新建一个Disruptor实例
        Disruptor<PipelinerEvent> disruptor = new Disruptor<PipelinerEvent>(
                PipelinerEvent.FACTORY, 1024, DaemonThreadFactory.INSTANCE);

        //这用使用示例多线程并发消费
        disruptor.handleEventsWith(
                //事件处理器
                new ParallelHandler(0, 3),
                new ParallelHandler(1, 3),
                new ParallelHandler(2, 3)
        ).then(new JoiningHandler());

        //启动
        RingBuffer<PipelinerEvent> ringBuffer = disruptor.start();

        for (int i = 0; i < 20; i++) {
            //获取下一个可用的发布数据位置
            long next = ringBuffer.next();
            try {
                //获取当前位置的事件，由于Disruptor提前初始化数组中的数据所有不需要判断
                PipelinerEvent pipelinerEvent = ringBuffer.get(next);
                //根据自己业务调整事件携带的数据
                pipelinerEvent.input = i;
            } finally {
                //发布事件
                ringBuffer.publish(next);
            }
        }

        TimeUnit.MILLISECONDS.sleep(30_0000);
    }

    /***
     *@className Pipeliner
     *
     *@description 多线程并行处理，根据线程数量分割数据到不同线程去处理
     *
     *@author <a href="http://youngitman.tech">青年IT男</a>
     *
     *@date 22:43 2020-02-04
     *
     *@JunitTest: {@link  }
     *
     *@version v1.0.0
     *
     **/
    private static class ParallelHandler implements EventHandler<PipelinerEvent> {
        private final int ordinal;
        private final int totalHandlers;

        ParallelHandler(int ordinal, int totalHandlers) {
            this.ordinal = ordinal;
            this.totalHandlers = totalHandlers;
        }

        @Override
        public void onEvent(PipelinerEvent event, long sequence, boolean endOfBatch) throws Exception {
            //过滤不需要处理的数据
            if (sequence % totalHandlers == ordinal) {
                event.result = "The thread name is " + Thread.currentThread().getName() + " and The Data is " + event.input;
            } else {
                //数据将被JoiningHandler过滤掉
            }
        }
    }

    private static class JoiningHandler implements EventHandler<PipelinerEvent> {
        private long lastEvent = -1;

        @Override
        public void onEvent(PipelinerEvent event, long sequence, boolean endOfBatch) throws Exception {
            //
            if (event.input != lastEvent + 1 || event.result == null) {
                System.out.println("Error: " + event);
            }
            System.out.println("The PipelinerEvent data is " + event + " for being handed");
            lastEvent = event.input;
            event.result = null;

        }
    }

    private static class PipelinerEvent {
        long input;
        Object result;

        private static final EventFactory<PipelinerEvent> FACTORY = new EventFactory<PipelinerEvent>() {
            @Override
            public PipelinerEvent newInstance() {
                return new PipelinerEvent();
            }
        };

        @Override
        public String toString() {
            return "PipelinerEvent{" +
                    "input=" + input +
                    ", result=" + result +
                    '}';
        }
    }
}
