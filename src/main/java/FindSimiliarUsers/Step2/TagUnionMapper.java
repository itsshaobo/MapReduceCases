package FindSimiliarUsers.Step2;

import FindSimiliarUsers.MyPair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TagUnionMapper extends Mapper<MyPair, IntWritable, MyPair, Text> {
    //used to process union Step3Data , tag Step3Data with 1

    private Text tagCount = new Text();

    @Override
    public void map(MyPair key, IntWritable value, Context context)
            throws IOException, InterruptedException {
        String tmp = 1 + "," + value.get();
        tagCount.set(tmp);
        context.write(key, tagCount);
    }
}

class TagInterMapper extends Mapper<MyPair, IntWritable, MyPair, Text> {
    //used to process union Step3Data , tag Step3Data with 1

    private Text tagCount = new Text();

    @Override
    public void map(MyPair key, IntWritable value, Context context)
            throws IOException, InterruptedException {
        String tmp = 0 + "," + value.get();
        tagCount.set(tmp);
        context.write(key, tagCount);
    }
}


class DenoReducer extends Reducer<MyPair, Text, MyPair, IntWritable> {

    private IntWritable finalNum = new IntWritable();

    @Override
    public void reduce(MyPair key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        List<String> helper = new ArrayList<String>();
        for(Text val:values){
            helper.add(val.toString());
        }
        int count;
        if(helper.size()==1) {
           String [] tmp = helper.get(0).split(",");
           count = Integer.valueOf(tmp[1]);
        }else{
            String [] tc1 = helper.get(0).split(",");
            String [] tc2 = helper.get(1).split(",");
            if(tc1[0].equals("1")){
                count = Integer.valueOf(tc1[1]) - Integer.valueOf(tc2[1]);
            }else{
                count = Integer.valueOf(tc2[1]) - Integer.valueOf(tc1[1]);
            }
        }
        finalNum.set(count);
        context.write(key,finalNum);
    }
}
