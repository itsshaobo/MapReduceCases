package FindSimiliarUsers.Step2;

import FindSimiliarUsers.MyPair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class InteractionMapper1 extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

    private IntWritable movieId = new IntWritable();
    private IntWritable userId = new IntWritable();

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String [] tmp = value.toString().split(",");
        if(tmp.length==3&&tmp[0].charAt(0)>='0'&&tmp[0].charAt(0)<='9'){
            movieId.set(Integer.valueOf(tmp[1]));
            userId.set(Integer.valueOf(tmp[0]));
            context.write(movieId, userId);
        }
    }
}

class InteractionReducer1 extends Reducer<IntWritable, IntWritable, MyPair, IntWritable> {

    private final List<Integer> helper = new ArrayList<Integer>();
    private MyPair userPairs = new MyPair();
    private static final IntWritable one = new IntWritable(1);

    @Override
    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        try{
            String pair;
            for(IntWritable elem:values){
                int n = helper.size();
                int user1 = elem.get();
                for(int i= 0;i< n;i++){
                    int user2 = helper.get(i);
                    if(user1!=user2){
                        if(user1>user2){
                            userPairs.set(user2, user1);
                        }else{
                            userPairs.set(user1,user2);
                        }
                        context.write(userPairs, one);
                    }
                }
                helper.add(user1);
            }
        }finally{
            helper.clear();
        }

    }
}
