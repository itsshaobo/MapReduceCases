package MutualFriends;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Step1 {

    public static class Mapper1 extends Mapper<LongWritable, Text, Text, Text> {
        private Text user = new Text();
        private Text friend = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String [] data = value.toString().split(":");
            user.set(data[0]);
            for(String elem:data[1].split(",")){
                friend.set(elem);
                context.write(friend,user);
            }
        }
    }

    public static class Reducer1 extends Reducer<Text, Text, Text, Text> {
        private Text friendsPair = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context )
                throws IOException, InterruptedException {
            List<String> users = new ArrayList<String>();
            for(Text elem:values) users.add(elem.toString());
            Collections.sort(users);
            for(int i=0;i<users.size()-1;i++) {
                for(int j=i+1;j<users.size();j++) {
                    String mutalFriends = users.get(i)+","+users.get(j);
                    friendsPair.set(mutalFriends);
                    context.write(friendsPair, key);
                }
            }
        }
    }
}










