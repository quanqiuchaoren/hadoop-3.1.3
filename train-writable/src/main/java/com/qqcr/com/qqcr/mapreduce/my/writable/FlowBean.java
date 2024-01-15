package com.qqcr.com.qqcr.mapreduce.my.writable;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 如果想要自定义的对象在mapper和reducer之间传输，则需要将该对象实现Writable接口，并重写write和readFields方法。
 */
@Getter
@Setter
@NoArgsConstructor
public class FlowBean implements Writable {
    /**
     * 上行流量
     */
    private long upFlow;
    /**
     * 下行流量
     */
    private long downFlow;
    /**
     * 总流量 = 上行+下行流量
     */
    private long sumFlow;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(sumFlow);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        upFlow = dataInput.readLong();
        downFlow = dataInput.readLong();
        sumFlow = dataInput.readLong();
    }

    /**
     * 需要重新tostring方法，否则输出的就是这个类的全限定名称
     */
    @Override
    public String toString() {
        return upFlow + "\t" +
                downFlow + "\t" +
                sumFlow;
    }
}
