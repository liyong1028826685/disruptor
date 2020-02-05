package com.lmax.disruptor.example;

import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.support.LongEvent;
import com.lmax.disruptor.util.DaemonThreadFactory;

public class WaitForProcessing {
    public static class Consumer implements EventHandler<LongEvent> {
        @Override
        public void onEvent(LongEvent event, long sequence, boolean endOfBatch) throws Exception {

        }
    }

    public static void main(String[] args) throws InterruptedException {
        final Disruptor<LongEvent> disruptor = new Disruptor<>(
                LongEvent.FACTORY, 1024, DaemonThreadFactory.INSTANCE);

        Consumer firstConsumer = new Consumer();
        Consumer lastConsumer = new Consumer();
        disruptor.handleEventsWith(firstConsumer).then(lastConsumer);
        final RingBuffer<LongEvent> ringBuffer = disruptor.getRingBuffer();

        EventTranslator<LongEvent> translator = (event, sequence) -> event.set(sequence - 4);

        ringBuffer.tryPublishEvent(translator);

        waitForSpecificConsumer(disruptor, lastConsumer, ringBuffer);
        waitForRingBufferToBeIdle(ringBuffer);
    }

    /***
     *
     * 等待RingBuffer可生产数据容量等于容器的容量
     *
     * @author liyong
     * @date 11:58 2020-02-05
     * @param ringBuffer
     * @exception
     * @return void
     **/
    @SuppressWarnings("StatementWithEmptyBody")
    private static void waitForRingBufferToBeIdle(RingBuffer<LongEvent> ringBuffer) {
        while (ringBuffer.getBufferSize() - ringBuffer.remainingCapacity() != 0) {
            // Wait for priocessing...
        }
    }

    /***
     *
     * 等待最后一个消费者处理数据游标位置到达生产者的游标位置
     *
     * @author liyong
     * @date 11:49 2020-02-05
     * @param disruptor
 * @param lastConsumer
 * @param ringBuffer
     * @exception
     * @return void
     **/
    private static void waitForSpecificConsumer(
            Disruptor<LongEvent> disruptor,
            Consumer lastConsumer,
            RingBuffer<LongEvent> ringBuffer) {
        long lastPublishedValue;
        long sequenceValueFor;
        do {
            lastPublishedValue = ringBuffer.getCursor();
            //获取最后一个消费者消费元素Sequence的位置
            sequenceValueFor = disruptor.getSequenceValueFor(lastConsumer);
        }
        while (sequenceValueFor < lastPublishedValue);
    }
}
