/*
 * Copyright 2011 LMAX Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.lmax.disruptor;

import java.util.concurrent.locks.LockSupport;

import sun.misc.Unsafe;

import com.lmax.disruptor.util.Util;

/**
 * <p>Coordinator for claiming sequences for access to a data structure while tracking dependent {@link Sequence}s.
 * Suitable for use for sequencing across multiple publisher threads.</p>
 *
 * <p> * Note on {@link Sequencer#getCursor()}:  With this sequencer the cursor value is updated after the call
 * to {@link Sequencer#next()}, to determine the highest available sequence that can be read, then
 * {@link Sequencer#getHighestPublishedSequence(long, long)} should be used.</p>
 */
public final class MultiProducerSequencer extends AbstractSequencer
{
    private static final Unsafe UNSAFE = Util.getUnsafe();
    /*** int[]数组的在当前对象的其实内存地址 */
    private static final long BASE = UNSAFE.arrayBaseOffset(int[].class);
    /*** int[]数组中每个元素的所占用内存大小 */
    private static final long SCALE = UNSAFE.arrayIndexScale(int[].class);
    /*** 所有消费者的Sequence中最小的那一个的缓存 */
    private final Sequence gatingSequenceCache = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);

    // availableBuffer tracks the state of each ringbuffer slot
    // see below for more details on the approach
    /*** 每个生产者拥有RingBuffer的对应大小的数组，用来保存对应RingBuffer位置元素在第几个环周期上 */
    private final int[] availableBuffer;
    /*** 用于索引计算，类似数组取模获取数组索引位置 */
    private final int indexMask;
    /*** 获取对数以2为底的指数值，用于计算availableBuffer中的值 */
    private final int indexShift;

    /**
     * Construct a Sequencer with the selected wait strategy and buffer size.
     *
     * @param bufferSize   the size of the buffer that this will sequence over.
     * @param waitStrategy for those waiting on sequences.
     */
    public MultiProducerSequencer(int bufferSize, final WaitStrategy waitStrategy)
    {
        super(bufferSize, waitStrategy);
        availableBuffer = new int[bufferSize];
        indexMask = bufferSize - 1;
        indexShift = Util.log2(bufferSize);
        //初始化生产者本地RingBuffer映射每个Solt初始值
        initialiseAvailableBuffer();
    }

    /**
     * @see Sequencer#hasAvailableCapacity(int)
     */
    @Override
    public boolean hasAvailableCapacity(final int requiredCapacity)
    {
        return hasAvailableCapacity(gatingSequences, requiredCapacity, cursor.get());
    }

    /***
     *
     * 参考{@link com.lmax.disruptor.MultiProducerSequencer#next(int)}
     *
     * @author liyong
     * @date 16:44 2020-02-04
     * @param gatingSequences
     * @param requiredCapacity
     * @param cursorValue
     * @exception
     * @return boolean
     **/
    private boolean hasAvailableCapacity(Sequence[] gatingSequences, final int requiredCapacity, long cursorValue)
    {
        long wrapPoint = (cursorValue + requiredCapacity) - bufferSize;
        long cachedGatingSequence = gatingSequenceCache.get();

        if (wrapPoint > cachedGatingSequence || cachedGatingSequence > cursorValue)
        {
            long minSequence = Util.getMinimumSequence(gatingSequences, cursorValue);
            gatingSequenceCache.set(minSequence);

            if (wrapPoint > minSequence)
            {
                return false;
            }
        }

        return true;
    }

    /**
     * @see Sequencer#claim(long)
     */
    @Override
    public void claim(long sequence)
    {
        cursor.set(sequence);
    }

    /**
     * @see Sequencer#next()
     */
    @Override
    public long next()
    {
        return next(1);
    }

    /**
     * 获取下一个可插入数据的solt位置,这里可能会有多个生产线程并发访问使用cas操作
     * @see Sequencer#next(int)
     */
    @Override
    public long next(int n)
    {
        if (n < 1 || n > bufferSize)
        {
            throw new IllegalArgumentException("n must be > 0 and < bufferSize");
        }

        long current;
        long next;

        do
        {
            //获取当前RingBuffer的cursor游标位置
            current = cursor.get();
            //获取下个索引位置
            next = current + n;
            //可认为是：下一个要生产的数据的位置。例如bufferSize=4，这里的分布是：-4，-3，-2，-1，0，1，2，3，4，5，6，7，8 ，so on是不是很像数组的下标位置
            long wrapPoint = next - bufferSize;
            //获取最慢消费线程的游标
            long cachedGatingSequence = gatingSequenceCache.get();
            /**
             *  1.下一个生产数据位置大于消费最慢线程位置
             *  2.消费最慢线程位置大于当前生产游标位置，则进行自旋。
             *  总结：每当生产下一个周期数据，需要等待消费者把当前周期内元素消费完成
             */
            if (wrapPoint > cachedGatingSequence || cachedGatingSequence > current)
            {
                //从所有消费者Sequence中获取游标值和current取最小值 min(current,Sequence[n].value)
                long gatingSequence = Util.getMinimumSequence(gatingSequences, current);
                //需要阻塞1纳秒，等待其他消费线程消费数据更新各自的Sequence游标到所有消费者最小游标大于或等于wrapPoint停止自旋。
                if (wrapPoint > gatingSequence)
                {
                    //最慢消费线程没有跟随生产线程
                    LockSupport.parkNanos(1);
                    continue;
                }
                //不断更新消费最慢线程的游标位置
                gatingSequenceCache.set(gatingSequence);
            }
            else if (cursor.compareAndSet(current, next))
            {
                break;
            }
        }
        while (true);

        return next;
    }

    /**
     * 尝试去获取下一个cursor位置，不会被阻塞，但是没有可用的位置会报错
     *
     * @see Sequencer#tryNext()
     */
    @Override
    public long tryNext() throws InsufficientCapacityException
    {
        return tryNext(1);
    }

    /**
     * @see Sequencer#tryNext(int)
     */
    @Override
    public long tryNext(int n) throws InsufficientCapacityException
    {
        if (n < 1)
        {
            throw new IllegalArgumentException("n must be > 0");
        }

        long current;
        long next;

        do
        {
            current = cursor.get();
            next = current + n;

            if (!hasAvailableCapacity(gatingSequences, n, current))
            {
                throw InsufficientCapacityException.INSTANCE;
            }
        }
        while (!cursor.compareAndSet(current, next));

        return next;
    }

    /**
     * RingBuffer中可以用于生产数据的容量
     * @see Sequencer#remainingCapacity()
     */
    @Override
    public long remainingCapacity()
    {
        //获取消费者中游标最小的值（生产者游标正常情况>=消费者游标）
        long consumed = Util.getMinimumSequence(gatingSequences, cursor.get());
        long produced = cursor.get();
        return getBufferSize() - (produced - consumed);
    }

    private void initialiseAvailableBuffer()
    {
        for (int i = availableBuffer.length - 1; i != 0; i--)
        {
            setAvailableBufferValue(i, -1);
        }

        setAvailableBufferValue(0, -1);
    }

    /**
     * @see Sequencer#publish(long)
     */
    @Override
    public void publish(final long sequence)
    {
        setAvailable(sequence);
        //通知被阻塞的消费者继续消费
        waitStrategy.signalAllWhenBlocking();
    }

    /**
     * 发布一段长度数据
     * @see Sequencer#publish(long, long)
     */
    @Override
    public void publish(long lo, long hi)
    {
        for (long l = lo; l <= hi; l++)
        {
            setAvailable(l);
        }
        waitStrategy.signalAllWhenBlocking();
    }

    /**
     *
     * 主要作用是记录cursor围绕RingBuffer旋转周期次数和RingBuffer每个元素位置一一对于
     *
     * The below methods work on the availableBuffer flag.
     * <p>
     * The prime reason is to avoid a shared sequence object between publisher threads.
     * (Keeping single pointers tracking start and end would require coordination
     * between the threads).
     * <p>
     * --  Firstly we have the constraint that the delta between the cursor and minimum
     * gating sequence will never be larger than the buffer size (the code in
     * next/tryNext in the Sequence takes care of that).
     * -- Given that; take the sequence value and mask off the lower portion of the
     * sequence as the index into the buffer (indexMask). (aka modulo operator)
     * -- The upper portion of the sequence becomes the value to check for availability.
     * ie: it tells us how many times around the ring buffer we've been (aka division)
     * -- Because we can't wrap without the gating sequences moving forward (i.e. the
     * minimum gating sequence is effectively our last available position in the
     * buffer), when we have new data and successfully claimed a slot we can simply
     * write over the top.
     */
    private void setAvailable(final long sequence)
    {
        setAvailableBufferValue(calculateIndex(sequence), calculateAvailabilityFlag(sequence));
    }

    /***
     *
     * 设置本地的availableBuffer的环周期数
     *
     * @author liyong
     * @date 14:41 2020-02-04
     * @param index
 * @param flag
     * @exception
     * @return void
     **/
    private void setAvailableBufferValue(int index, int flag)
    {
        long bufferAddress = (index * SCALE) + BASE;
        //延迟赋值，不保证新的赋值能立即被其他线程获取到
        UNSAFE.putOrderedInt(availableBuffer, bufferAddress, flag);
    }

    /**
     * 确定sequence已经被发布了，并且event事件是有效的，确定在同一个环周期数
     * @see Sequencer#isAvailable(long)
     */
    @Override
    public boolean isAvailable(long sequence)
    {
        //元素位置
        int index = calculateIndex(sequence);
        //通过sequence计算出环周期数
        int flag = calculateAvailabilityFlag(sequence);
        //sequence元素在availableBuffer数组中的位置
        long bufferAddress = (index * SCALE) + BASE;
        //获得给定对象的指定偏移量offset的int值，使用volatile语义，总能获取到最新的int值。
        return UNSAFE.getIntVolatile(availableBuffer, bufferAddress) == flag;
    }

    /***
     *
     * 获取lowerBound到availableSequence中有效sequence且最大
     *
     * @author liyong
     * @date 19:43 2020-02-03
     * @param lowerBound
     * @param availableSequence
     * @exception
     * @return long
     **/
    @Override
    public long getHighestPublishedSequence(long lowerBound, long availableSequence)
    {
        for (long sequence = lowerBound; sequence <= availableSequence; sequence++)
        {
            if (!isAvailable(sequence))
            {
                return sequence - 1;
            }
        }

        return availableSequence;
    }

    /***
     *
     * 就是这个环（RingBuffer）循环第几周（圈）
     *
     * @author liyong
     * @date 20:30 2020-02-03
     * @param
     * @exception
     * @return int
     **/
    private int calculateAvailabilityFlag(final long sequence)
    {
        return (int) (sequence >>> indexShift);
    }

    /***
     *
     * sequence在数组中的索引位置，类似求模运算 序号%数组长度
     *
     * @author liyong
     * @date 19:52 2020-02-03
     * @param
     * @exception
     * @return int
     **/
    private int calculateIndex(final long sequence)
    {
        return ((int) sequence) & indexMask;
    }
}
