package com.lmax.disruptor;

import com.lmax.disruptor.util.Util;

/**
 * @author <a href="http://youngitman.tech">青年IT男</a>
 * @version v1.0.0
 * @className Test
 * @description
 * @date 2020-02-03 19:57
 * @JunitTest: {@link  }
 **/
public class Test {

    static int indexShift;

    public static void main(String[] args) {
        int bufferSize = 1024;//1024 * 2
        //2^n < bufferSize n = 3
        indexShift = Util.log2(bufferSize);

        System.out.println("indexShift=>" + indexShift);
//        for (long i = 0; i < 50; i++) {
//            int r = calculateAvailabilityFlag(i);
//            System.out.println("result=>" + r);
//        }

        int r = calculateAvailabilityFlag(2050);
        System.out.println("r=>"+r);
    }

    private static int calculateAvailabilityFlag(final long sequence) {
        return (int) (sequence >>> indexShift);
    }
}
