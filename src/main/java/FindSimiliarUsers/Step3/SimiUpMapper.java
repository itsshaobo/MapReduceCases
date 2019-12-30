package FindSimiliarUsers.Step3;

import FindSimiliarUsers.MyPair;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class SimiUpMapper extends Mapper<LongWritable, Text, MyPair, Text> {
    //used to process numerator , tag Step3Data with 0

    private MyPair userPairs = new MyPair();
    private Text tagCount = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String [] tmp = value.toString().split(":");
        String [] users = tmp[0].split(",");
        userPairs.set(Integer.valueOf(users[0]), Integer.valueOf(users[1]));
        tagCount.set("0," + tmp[1]);
        context.write(userPairs, tagCount);
    }
}

class SimiDownMapper extends Mapper<LongWritable, Text, MyPair, Text> {
    //used to process denominator Step3Data , tag Step3Data with 1

    private MyPair userPairs = new MyPair();
    private Text tagCount = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String [] tmp = value.toString().split("\t");
        String [] users = tmp[0].split(",");
        userPairs.set(Integer.valueOf(users[0]), Integer.valueOf(users[1]));
        tagCount.set("1," + tmp[1]);
        context.write(userPairs, tagCount);
    }
}

class SimiReducer extends Reducer<MyPair, Text, MyPair, FloatWritable> {

    private FloatWritable finalNum = new FloatWritable();

    @Override
    public void reduce(MyPair key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        List<String> helper = new ArrayList<String>();
        for(Text val:values){
            helper.add(val.toString());
        }
        float count;
        if(helper.size()==2) {
            String [] tc1 = helper.get(0).split(",");
            String [] tc2 = helper.get(1).split(",");
            if(tc1[0].equals("0")){
                count = (float)Integer.valueOf(tc1[1]) / Integer.valueOf(tc2[1]);
            }else{
                count = (float)Integer.valueOf(tc2[1]) / Integer.valueOf(tc1[1]);
            }
            finalNum.set(count);
            context.write(key,finalNum);
        }
    }
}
