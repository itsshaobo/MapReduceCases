package FindSimiliarUsers.Step1;

import FindSimiliarUsers.MyPair;
import FindSimiliarUsers.PairCount;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortCountMapper extends Mapper<MyPair, IntWritable, PairCount, NullWritable> {

    private PairCount pc = new PairCount();

    public void map(MyPair key, IntWritable value, Context context)
            throws IOException, InterruptedException {
        pc.set(key.getUser1().get(), key.getUser2().get() , value.get());
        context.write(pc,NullWritable.get());
    }
}

class SortCountReducer extends Reducer<PairCount, NullWritable, PairCount, NullWritable> {

    public void reduce(PairCount key, Iterable<NullWritable> values, Context context)
            throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
    }
}