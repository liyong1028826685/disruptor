package com.lmax.disruptor.dsl;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;

/***
 *@className BasicExecutor
 *
 *@description
 *
 * 封装 {@link java.lang.Runnable},并通过队列来持有所有执行线程以便后面对线程进行监控
 *
 *@author <a href="http://youngitman.tech">青年IT男</a>
 *
 *@date 17:41 2020-02-03
 *
 *@JunitTest: {@link  }
 *
 *@version v1.0.0
 *
**/
public class BasicExecutor implements Executor
{
    private final ThreadFactory factory;
    private final Queue<Thread> threads = new ConcurrentLinkedQueue<>();

    public BasicExecutor(ThreadFactory factory)
    {
        this.factory = factory;
    }

    @Override
    public void execute(Runnable command)
    {
        final Thread thread = factory.newThread(command);
        if (null == thread)
        {
            throw new RuntimeException("Failed to create thread to run: " + command);
        }

        thread.start();

        threads.add(thread);
    }

    @Override
    public String toString()
    {
        return "BasicExecutor{" +
            "threads=" + dumpThreadInfo() +
            '}';
    }

    /***
     *
     * 获取消费执行线程信息
     *
     * @author liyong
     * @date 17:44 2020-02-03
     *  * @param
     * @exception
     * @return java.lang.String
     **/
    private String dumpThreadInfo()
    {
        final StringBuilder sb = new StringBuilder();

        final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();

        for (Thread t : threads)
        {
            ThreadInfo threadInfo = threadMXBean.getThreadInfo(t.getId());
            sb.append("{");
            sb.append("name=").append(t.getName()).append(",");
            sb.append("id=").append(t.getId()).append(",");
            sb.append("state=").append(threadInfo.getThreadState()).append(",");
            sb.append("lockInfo=").append(threadInfo.getLockInfo());
            sb.append("}");
        }

        return sb.toString();
    }
}
