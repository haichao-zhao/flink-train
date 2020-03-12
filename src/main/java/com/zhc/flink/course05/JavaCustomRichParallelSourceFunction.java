package com.zhc.flink.course05;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class JavaCustomRichParallelSourceFunction extends RichParallelSourceFunction<Long> {

    boolean isRunning = true;
    Long count = 1L;

    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        while (isRunning) {
            ctx.collect(count);
            count++;
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
