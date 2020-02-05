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

import java.util.concurrent.atomic.AtomicInteger;


/**
 * Convenience class for handling the batching semantics of consuming entries from a {@link RingBuffer}
 * and delegating the available events to an {@link EventHandler}.
 * <p>
 * If the {@link EventHandler} also implements {@link LifecycleAware} it will be notified just after the thread
 * is started and just before the thread is shutdown.
 *
 * @param <T> event implementation storing the data for sharing during exchange or parallel coordination of an event.
 */
public final class BatchEventProcessor<T>
    implements EventProcessor
{
    private static final int IDLE = 0;
    private static final int HALTED = IDLE + 1;
    private static final int RUNNING = HALTED + 1;

    private final AtomicInteger running = new AtomicInteger(IDLE);
    private ExceptionHandler<? super T> exceptionHandler = new FatalExceptionHandler();
    private final DataProvider<T> dataProvider;
    private final SequenceBarrier sequenceBarrier;
    private final EventHandler<? super T> eventHandler;
    private final Sequence sequence = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);
    private final TimeoutHandler timeoutHandler;
    private final BatchStartAware batchStartAware;

    /**
     * Construct a {@link EventProcessor} that will automatically track the progress by updating its sequence when
     * the {@link EventHandler#onEvent(Object, long, boolean)} method returns.
     *
     * @param dataProvider    to which events are published.
     * @param sequenceBarrier on which it is waiting.
     * @param eventHandler    is the delegate to which events are dispatched.
     */
    public BatchEventProcessor(
        final DataProvider<T> dataProvider,
        final SequenceBarrier sequenceBarrier,
        final EventHandler<? super T> eventHandler)
    {
        this.dataProvider = dataProvider;
        this.sequenceBarrier = sequenceBarrier;
        this.eventHandler = eventHandler;

        if (eventHandler instanceof SequenceReportingEventHandler)
        {
            ((SequenceReportingEventHandler<?>) eventHandler).setSequenceCallback(sequence);
        }

        batchStartAware =
            (eventHandler instanceof BatchStartAware) ? (BatchStartAware) eventHandler : null;
        timeoutHandler =
            (eventHandler instanceof TimeoutHandler) ? (TimeoutHandler) eventHandler : null;
    }

    @Override
    public Sequence getSequence()
    {
        return sequence;
    }

    @Override
    public void halt()
    {
        running.set(HALTED);
        sequenceBarrier.alert();
    }

    @Override
    public boolean isRunning()
    {
        return running.get() != IDLE;
    }

    /**
     * 设置一个自定义的ExceptionHandler
     * Set a new {@link ExceptionHandler} for handling exceptions propagated out of the {@link BatchEventProcessor}
     *
     * @param exceptionHandler to replace the existing exceptionHandler.
     */
    public void setExceptionHandler(final ExceptionHandler<? super T> exceptionHandler)
    {
        if (null == exceptionHandler)
        {
            throw new NullPointerException();
        }

        this.exceptionHandler = exceptionHandler;
    }

    /**
     * It is ok to have another thread rerun this method after a halt().
     *
     * @throws IllegalStateException if this object instance is already running in a thread
     */
    @Override
    public void run()
    {
        //判断是否IDLE状态，并设置为RUNNING状态成功则可以正常启动执行
        if (running.compareAndSet(IDLE, RUNNING))
        {
            //设置alerted=false
            sequenceBarrier.clearAlert();
            //判断EventHandler是否实现LifecycleAware生命周期接口
            notifyStart();
            try
            {
                if (running.get() == RUNNING)
                {
                    //开始处理RingBuffer中的Event数据
                    processEvents();
                }
            }
            finally
            {
                //判断EventHandler是否实现LifecycleAware生命周期接口
                notifyShutdown();
                //这种当前Processor状态为IDLE
                running.set(IDLE);
            }
        }
        else
        {
            // This is a little bit of guess work.  The running state could of changed to HALTED by
            // this point.  However, Java does not have compareAndExchange which is the only way
            // to get it exactly correct.
            if (running.get() == RUNNING)
            {
                throw new IllegalStateException("Thread is already running");
            }
            else
            {
                //不处理RingBuffer中的Event数据，直接执行回掉LifecycleAware接口
                earlyExit();
            }
        }
    }

    /***
     *
     * 处理RingBuffer中的Event数据
     *
     * @author liyong
     * @date 18:47 2020-02-03
     *  * @param
     * @exception
     * @return void
     **/
    private void processEvents()
    {
        T event = null;
        //当前Processor的本地sequence初始化为-1，首次执行nextSequence从0开始
        long nextSequence = sequence.get() + 1L;

        while (true)
        {
            try
            {
                //获取或等待一个有效的Sequence，Note：availableSequence是指环上有效的数据位置
                final long availableSequence = sequenceBarrier.waitFor(nextSequence);
                //availableSequence >= nextSequence 说明有数据可以消费
                if (batchStartAware != null && availableSequence >= nextSequence)
                {
                    //作一个onBatchStart回掉
                    batchStartAware.onBatchStart(availableSequence - nextSequence + 1);
                }

                //从本地nextSequence开始到availableSequence开始消费这个区间数据
                while (nextSequence <= availableSequence)
                {
                    //从RingBuffer获取Event数据，外面对暑假见修改会影响到数组元素
                    event = dataProvider.get(nextSequence);
                    //回掉我们消费自定义的EventHandler
                    eventHandler.onEvent(event, nextSequence, nextSequence == availableSequence);
                    nextSequence++;
                }
                //设置当前消费者Processor的sequence为availableSequence位置，注意这里sequence是一直递增的数据
                sequence.set(availableSequence);
            }
            catch (final TimeoutException e)
            {
                //超时回掉，nextSequence不会自增，在收到这种通知的时候需要业务判断是否重复消费数据
                notifyTimeout(sequence.get());
            }
            catch (final AlertException ex)
            {
                //回终止消费者继续消费
                if (running.get() != RUNNING)
                {
                    break;
                }
            }
            catch (final Throwable ex)
            {
                //未知异常交个异常处理器，继续执行后面数据操作，由业务来判断数据后续处理
                exceptionHandler.handleEventException(ex, nextSequence, event);
                sequence.set(nextSequence);
                nextSequence++;
            }
        }
    }

    private void earlyExit()
    {
        notifyStart();
        notifyShutdown();
    }

    private void notifyTimeout(final long availableSequence)
    {
        try
        {
            if (timeoutHandler != null)
            {
                timeoutHandler.onTimeout(availableSequence);
            }
        }
        catch (Throwable e)
        {
            exceptionHandler.handleEventException(e, availableSequence, null);
        }
    }

    /**
     * Notifies the EventHandler when this processor is starting up
     */
    private void notifyStart()
    {
        if (eventHandler instanceof LifecycleAware)
        {
            try
            {
                //执行onStart回掉
                ((LifecycleAware) eventHandler).onStart();
            }
            catch (final Throwable ex)
            {
                exceptionHandler.handleOnStartException(ex);
            }
        }
    }

    /**
     * Notifies the EventHandler immediately prior to this processor shutting down
     */
    private void notifyShutdown()
    {
        if (eventHandler instanceof LifecycleAware)
        {
            try
            {
                //执行onShutdown回掉
                ((LifecycleAware) eventHandler).onShutdown();
            }
            catch (final Throwable ex)
            {
                exceptionHandler.handleOnShutdownException(ex);
            }
        }
    }
}