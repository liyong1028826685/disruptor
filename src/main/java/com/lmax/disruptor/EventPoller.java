package com.lmax.disruptor;

/**
 * Experimental poll-based interface for the Disruptor.
 */
public class EventPoller<T>
{
    private final DataProvider<T> dataProvider;
    private final Sequencer sequencer;
    /*** Poller的本地游标类比消费者本地游标 */
    private final Sequence sequence;
    /*** 当前Poller消费数据是所依赖的其他所有Poller或消费者（类似链式处理，必须前面一个handler处理了后面的）handler才能处理*/
    private final Sequence gatingSequence;

    public interface Handler<T>
    {
        boolean onEvent(T event, long sequence, boolean endOfBatch) throws Exception;
    }

    public enum PollState
    {
        PROCESSING, GATING, IDLE
    }

    public EventPoller(
        final DataProvider<T> dataProvider,
        final Sequencer sequencer,
        final Sequence sequence,
        final Sequence gatingSequence)
    {
        this.dataProvider = dataProvider;
        this.sequencer = sequencer;
        this.sequence = sequence;
        this.gatingSequence = gatingSequence;
    }

    /**
     * 每次poll会把所有有效数据全部获取
     * @param eventHandler
     * @return
     * @throws Exception
     */
    public PollState poll(final Handler<T> eventHandler) throws Exception
    {
        final long currentSequence = sequence.get();
        //获取下一个游标位置
        long nextSequence = currentSequence + 1;
        //获取有效的Sequence
        final long availableSequence = sequencer.getHighestPublishedSequence(nextSequence, gatingSequence.get());
        //如果Poller的Sequence<=availableSequence说明有数据可以消费
        if (nextSequence <= availableSequence)
        {
            boolean processNextEvent;
            long processedSequence = currentSequence;

            try
            {
                //循环开始获取数据，并且移动本地Sequence游标
                do
                {
                    final T event = dataProvider.get(nextSequence);
                    processNextEvent = eventHandler.onEvent(event, nextSequence, nextSequence == availableSequence);
                    processedSequence = nextSequence;
                    nextSequence++;

                }
                while (nextSequence <= availableSequence & processNextEvent);
            }
            finally
            {
                //设置本地的Sequence游标到处理完数据的位置，以便后续继续消费
                sequence.set(processedSequence);
            }

            return PollState.PROCESSING;
        }
        else if (sequencer.getCursor() >= nextSequence)
        {
            //没有数据消费
            return PollState.GATING;
        }
        else
        {
            //空闲
            return PollState.IDLE;
        }
    }

    public static <T> EventPoller<T> newInstance(
        final DataProvider<T> dataProvider,
        final Sequencer sequencer,
        final Sequence sequence,
        final Sequence cursorSequence,
        final Sequence... gatingSequences)
    {
        Sequence gatingSequence;
        if (gatingSequences.length == 0)
        {
            gatingSequence = cursorSequence;
        }
        else if (gatingSequences.length == 1)
        {
            gatingSequence = gatingSequences[0];
        }
        else
        {
            gatingSequence = new FixedSequenceGroup(gatingSequences);
        }

        return new EventPoller<T>(dataProvider, sequencer, sequence, gatingSequence);
    }

    public Sequence getSequence()
    {
        return sequence;
    }
}
