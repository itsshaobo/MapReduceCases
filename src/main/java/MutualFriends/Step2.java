package MutualFriends;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Step2 {

    public static class Mapper2 extends Mapper<Text, Text ,Text, Text> {

        public void map(Text key ,Text value, Context context)
                throws IOException, InterruptedException {
            context.write(key,value);
        }
    }
}



