package demo2;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DataBean implements Writable {
    private String telNo;
    private long upPayLoad;
    private long downPayLoad;
    private long totalPayLoad;

    public DataBean() {
    }

    public DataBean(String telNo, long upPayLoad, long downPayLoad) {
        super();
        this.telNo = telNo;
        this.upPayLoad = upPayLoad;
        this.downPayLoad = downPayLoad;
        this.totalPayLoad = upPayLoad+downPayLoad;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(telNo);
        dataOutput.writeLong(upPayLoad);
        dataOutput.writeLong(downPayLoad);
        dataOutput.writeLong(totalPayLoad);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.telNo = dataInput.readUTF();
        this.upPayLoad = dataInput.readLong();
        this.downPayLoad = dataInput.readLong();
        this.totalPayLoad = dataInput.readLong();

    }

    @Override
    public String toString() {
        return this.upPayLoad+"\t"+this.downPayLoad+"\t"+this.totalPayLoad;
    }

    public String getTelNo() {
        return telNo;
    }

    public void setTelNo(String telNo) {
        this.telNo = telNo;
    }

    public long getUpPayLoad() {
        return upPayLoad;
    }

    public void setUpPayLoad(long upPayLoad) {
        this.upPayLoad = upPayLoad;
    }

    public long getDownPayLoad() {
        return downPayLoad;
    }

    public void setDownPayLoad(long downPayLoad) {
        this.downPayLoad = downPayLoad;
    }

    public long getTotalPayLoad() {
        return totalPayLoad;
    }

    public void setTotalPayLoad(long totalPayLoad) {
        this.totalPayLoad = totalPayLoad;
    }
}
