package FindSimiliarUsers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MyPair implements WritableComparable<MyPair> {

    private IntWritable user1;
    private IntWritable user2;

    public MyPair(){
        user1 = new IntWritable();
        user2 = new IntWritable();
    }

    public MyPair(int us1, int us2){
        user1 = new IntWritable(us1);
        user2 = new IntWritable(us2);
    }

    public void set(int us1, int us2) {
        user1.set(us1);
        user2.set(us2);
    }

    public IntWritable getUser1(){ return user1; }

    public IntWritable getUser2(){ return user2; }

    @Override
    public void write(DataOutput dout) throws IOException {
        user1.write(dout);
        user2.write(dout);
    }

    @Override
    public void readFields(DataInput din) throws IOException {
        user1.readFields(din);
        user2.readFields(din);
    }

    @Override
    public int compareTo(MyPair p){
        int tmp = user1.compareTo(p.user1);
        if(tmp==0){
            return user2.compareTo(p.user2);
        }
        return tmp;
    }

    @Override
    public String toString(){
        return user1 + "," + user2;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof MyPair){
            MyPair tmp = (MyPair)obj;
            return user1.equals(tmp.user1) && user2.equals(tmp.user2);
        }
        return false;
    }

    @Override
    public int hashCode(){
        return user1.hashCode();
    }
}
