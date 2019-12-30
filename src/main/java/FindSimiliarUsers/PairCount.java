package FindSimiliarUsers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class PairCount implements WritableComparable<PairCount> {

    private MyPair pair;
    private IntWritable count;

    public PairCount(){
        pair = new MyPair();
        count = new IntWritable();
    }

    public PairCount(int user1, int user2, int count){
        this.pair = new MyPair(user1, user2);
        this.count = new IntWritable(count);
    }

    public void set(int user1, int user2, int count){
        this.pair.set(user1, user2);
        this.count.set(count);
    }

    @Override
    public void write(DataOutput dout) throws IOException {
        pair.write(dout);
        count.write(dout);
    }

    @Override
    public void readFields(DataInput din) throws IOException {
        pair.readFields(din);
        count.readFields(din);
    }

    public MyPair getPair() { return pair; }

    public IntWritable getCount() { return count; }

    public int compareTo(PairCount pc){
        int tmp = pc.count.compareTo(count);
        if(tmp==0){
            return pc.pair.compareTo(pair);
        }
        return tmp;
    }

    @Override
    public String toString(){
        return pair + ":" + count;
    }

    @Override
    public boolean equals(Object obj){
        if(obj instanceof PairCount){
            PairCount tmp = (PairCount)obj;
            if(tmp.getCount().equals(count) && tmp.getPair().equals(pair)){
                return true;
            }
        }
        return false;
    }

    @Override
    public int hashCode(){
        return pair.hashCode();
    }
}