package com.lmax.disruptor.example;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventPoller;
import com.lmax.disruptor.RingBuffer;

/**
 * Created by barkerm on 02/02/15.
 */
public class PullWithPoller {
    public static class DataEvent<T> {
        T data;

        public static <T> EventFactory<DataEvent<T>> factory() {
            return () -> new DataEvent<T>();
        }

        /***
         *
         * 如果数据直接引用的话则会修改RingBuffer中真实数据，so这里进行数据的copy
         *
         * @author liyong
         * @date 10:24 2020-02-05
         * @param
         * @exception
         * @return T
         **/
        public T copyOfData()   {
            // Copy the data out here.  In this case we have a single reference object, so the pass by
            // reference is sufficient.  But if we were reusing a byte array, then we would need to copy
            // the actual contents.
            return data;
        }
    }

    public static void main(String[] args) throws Exception {
        RingBuffer<DataEvent<Object>> ringBuffer = RingBuffer.createMultiProducer(DataEvent.factory(), 1024);

        final EventPoller<DataEvent<Object>> poller = ringBuffer.newPoller();

        Object value = getNextValue(poller);

        // Value could be null if no events are available.
        if (null != value) {
            // Process value.
        }
    }

    private static Object getNextValue(EventPoller<DataEvent<Object>> poller) throws Exception {
        final Object[] out = new Object[1];

        poller.poll(
                (event, sequence, endOfBatch) -> {
                    out[0] = event.copyOfData();

                    // Return false so that only one event is processed at a time.
                    return false;
                });

        return out[0];
    }
}
