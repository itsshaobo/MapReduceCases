package FindSimiliarUsers.Step2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class UnionMapper1 extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
    /**
     * input Step3Data is from initial Step3Data set, only extract userID as one record
     * output key = userID, value = 1
     * */
    private IntWritable userId = new IntWritable();
    private static final IntWritable one = new IntWritable(1);

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String [] tmp = value.toString().split(",");
        if(tmp.length==3 && tmp[0].charAt(0)>='0' && tmp[0].charAt(0)<='9'){
            userId.set(Integer.valueOf(tmp[0]));
            context.write(userId, one);
        }
    }
}

class UnionReducer1 extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    private IntWritable count = new IntWritable();

    @Override
    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for(IntWritable elem:values){
            sum++;
        }
        count.set(sum);
        context.write(key, count);

    }
}