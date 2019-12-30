package FindSimiliarUsers.Step2;

import FindSimiliarUsers.MyPair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class InteractionMapper2 extends Mapper<MyPair, IntWritable, MyPair, IntWritable> {

    public void map(MyPair key, IntWritable value, Context context)
            throws IOException, InterruptedException {
        context.write(key, value);
    }
}

class InteractionReducer2 extends Reducer<MyPair, IntWritable, MyPair, IntWritable> {

    private IntWritable count = new IntWritable();

    @Override
    public void reduce(MyPair key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for(IntWritable elem:values){
            sum++;
        }
        count.set(sum);
        context.write(key,count);
    }
}