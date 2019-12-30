package FindSimiliarUsers.Step1;

import FindSimiliarUsers.MyPair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CountPairMapper extends Mapper<MyPair, IntWritable, MyPair, IntWritable> {

    public void map(MyPair key, IntWritable value, Context context)
            throws IOException, InterruptedException {
        context.write(key, value);
    }
}

class CountPairReducer extends Reducer<MyPair, IntWritable, MyPair, IntWritable> {

    private IntWritable val = new IntWritable();

    public void reduce(MyPair key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int count = 0;
        for(IntWritable elem:values){
            count++;
        }
        val.set(count);
        context.write(key, val);
    }
}

