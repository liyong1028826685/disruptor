package com.lmax.disruptor.example;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;
/***
 *@className NamedEventHandler
 *
 *@description 给处理器当前线程命名，通过实现LifecycleAware接口在初始化回掉时候设置，在处理器退出是恢复原来名称
 *
 *@author <a href="http://youngitman.tech">青年IT男</a>
 *
 *@date 22:17 2020-02-04
 *
 *@JunitTest: {@link  }
 *
 *@version v1.0.0
 *
**/
public class NamedEventHandler<T> implements EventHandler<T>, LifecycleAware
{
    private String oldName;
    private final String name;

    public NamedEventHandler(final String name)
    {
        this.name = name;
    }

    @Override
    public void onEvent(final T event, final long sequence, final boolean endOfBatch) throws Exception
    {
    }

    @Override
    public void onStart()
    {
        final Thread currentThread = Thread.currentThread();
        oldName = currentThread.getName();
        currentThread.setName(name);
    }

    @Override
    public void onShutdown()
    {
        Thread.currentThread().setName(oldName);
    }
}
