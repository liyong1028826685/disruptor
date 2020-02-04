package com.lmax.disruptor.example;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.support.LongEvent;
import com.lmax.disruptor.util.DaemonThreadFactory;

/***
 *@className HandleExceptionOnTranslate
 *
 *@description 异常处理
 *
 *@author <a href="http://youngitman.tech">青年IT男</a>
 *
 *@date 21:59 2020-02-04
 *
 *@JunitTest: {@link  }
 *
 *@version v1.0.0
 *
**/
public class HandleExceptionOnTranslate
{
    private static final int NO_VALUE_SPECIFIED = -1;

    private static class MyHandler implements EventHandler<LongEvent>
    {

        @Override
        public void onEvent(LongEvent event, long sequence, boolean endOfBatch) throws Exception
        {
            //处理异常默认值
            if (event.get() == NO_VALUE_SPECIFIED)
            {
                System.out.printf("Discarded%n");
            }
            else
            {
                System.out.printf("Processed: %s%n", event.get() == sequence);
            }
        }
    }

    public static void main(String[] args) throws InterruptedException
    {
        Disruptor<LongEvent> disruptor = new Disruptor<LongEvent>(LongEvent.FACTORY, 1024, DaemonThreadFactory.INSTANCE);

        disruptor.handleEventsWith(new MyHandler());

        disruptor.start();

        EventTranslator<LongEvent> t = new EventTranslator<LongEvent>()
        {
            @Override
            public void translateTo(LongEvent event, long sequence)
            {
                //设置一个默认值，如果下面业务可能抛出异常
                event.set(NO_VALUE_SPECIFIED);

                if (sequence % 3 == 0)
                {
                    //发生异常直接抛出异常
                    throw new RuntimeException("Skipping");
                }

                event.set(sequence);
            }
        };

        for (int i = 0; i < 10; i++)
        {
            try
            {
                disruptor.publishEvent(t);
            }
            catch (RuntimeException e)
            {
                // Skipping
            }
        }

        Thread.sleep(5000);
    }
}
