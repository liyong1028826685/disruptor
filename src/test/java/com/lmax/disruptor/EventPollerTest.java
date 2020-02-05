package com.lmax.disruptor;

import com.lmax.disruptor.EventPoller.PollState;
import org.junit.Test;

import java.util.ArrayList;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/***
 *@className EventPollerTest
 *
 *@description 手动控制生产者游标和消费者游标模拟数据生产和消费
 *
 *@author <a href="http://youngitman.tech">青年IT男</a>
 *
 *@date 22:41 2020-02-05
 *
 *@JunitTest: {@link  }
 *
 *@version v1.0.0
 *
**/
public class EventPollerTest {
    @Test
    @SuppressWarnings("unchecked")
    public void shouldPollForEvents() throws Exception {
        final Sequence gatingSequence = new Sequence();
        final SingleProducerSequencer sequencer = new SingleProducerSequencer(16, new BusySpinWaitStrategy());
        final EventPoller.Handler<Object> handler = (event, sequence, endOfBatch) -> false;

        final Object[] data = new Object[16];
        final DataProvider<Object> provider = sequence -> data[(int) sequence];

        final EventPoller<Object> poller = sequencer.newPoller(provider, gatingSequence);
        final Object event = new Object();
        data[0] = event;

        //拉取有效事件
        assertThat(poller.poll(handler), is(PollState.IDLE));

        //发布一个事件
        sequencer.publish(sequencer.next());
        //拉取有效事件
        assertThat(poller.poll(handler), is(PollState.GATING));

        //增加消费者游标
        gatingSequence.incrementAndGet();
        //事件已经被处理
        assertThat(poller.poll(handler), is(PollState.PROCESSING));
    }

    @Test
    public void shouldSuccessfullyPollWhenBufferIsFull() throws Exception {
        final ArrayList<byte[]> events = new ArrayList<>();

        final EventPoller.Handler<byte[]> handler = (event, sequence, endOfBatch) -> {
            events.add(event);
            return !endOfBatch;
        };

        EventFactory<byte[]> factory = () -> new byte[1];

        final RingBuffer<byte[]> ringBuffer = RingBuffer.createMultiProducer(factory, 4, new SleepingWaitStrategy());

        final EventPoller<byte[]> poller = ringBuffer.newPoller();
        //设置消费者Sequence为poller.getSequence()
        ringBuffer.addGatingSequences(poller.getSequence());

        int count = 4;

        for (byte i = 1; i <= count; ++i) {
            long next = ringBuffer.next();
            ringBuffer.get(next)[0] = i;
            ringBuffer.publish(next);
        }

        // 多线程需要创建多个poller
        poller.poll(handler);

        assertThat(events.size(), is(4));
    }
}
