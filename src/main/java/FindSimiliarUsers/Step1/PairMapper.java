package FindSimiliarUsers.Step1;

import FindSimiliarUsers.MovieWithScore;
import FindSimiliarUsers.MyPair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;


public class PairMapper extends Mapper<LongWritable, Text, MovieWithScore, IntWritable> {

    private MovieWithScore movSco = new MovieWithScore();
    private IntWritable user = new IntWritable();

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String [] tmp = value.toString().split(",");
        if(tmp.length==3&&tmp[0].charAt(0)>='0'&&tmp[0].charAt(0)<='9'){
            int mov = Integer.valueOf(tmp[1]);
            float sco = Float.valueOf(tmp[2]);
            movSco.set(mov, sco);
            user.set(Integer.valueOf(tmp[0]));
            context.write(movSco, user);
        }
    }
}

class PairReducer extends Reducer<MovieWithScore, IntWritable, MyPair, IntWritable> {
    // the output <key, value> is  key =
    private List<Integer> helper = new ArrayList<Integer>();
    private MyPair userPair = new MyPair();
    private static final IntWritable num = new IntWritable(1);
    public void reduce(MovieWithScore key, Iterable<IntWritable> values, Context context)
            throws IOException,InterruptedException {
        try{
            for(IntWritable elem:values) {
                helper.add(elem.get());
            }
            int n = helper.size();
            String pair;
            for(int i=0;i<n;i++){
                int user2 = helper.get(i);
                for(int j=0;j<i;j++){
                    int user1 = helper.get(j);
                    if(user1>user2){ // make sure that users with smaller id be in the first position in pairs
                        userPair.set(user2, user1);
                    }else{
                        userPair.set(user1, user2);
                    }
                    context.write(userPair, num);
                }
            }
        }finally{
            helper.clear();
        }

    }
}