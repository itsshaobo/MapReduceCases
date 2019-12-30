package FindSimiliarUsers.Step2;

import FindSimiliarUsers.MyPair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UnionMapper2 extends Mapper<IntWritable, IntWritable, IntWritable, Text> {
    /**
     * 输入为 key = userId, value = count
     * 输出的 key = 1, value 为 Text类型， 输入的键值进行拼接   userId,count
     * */
    private Text userCount = new Text();
    private final static IntWritable one = new IntWritable(1);

    @Override
    public void map(IntWritable key, IntWritable value, Context context)
            throws IOException, InterruptedException {
        String tmp = key.get() + "," + value.get();
        userCount.set(tmp);
        context.write(one, userCount);
    }
}

class UnionReducer2 extends Reducer<IntWritable, Text, MyPair, IntWritable> {

    private MyPair userPairs = new MyPair();
    private IntWritable union = new IntWritable();

    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        Map<Integer, Integer> map = new HashMap<Integer, Integer>();
        //put <userID,count> into a map
        for(Text elem:values) {
            String[] tmp = elem.toString().split(",");
            if(tmp.length==2) map.put(Integer.valueOf(tmp[0]), Integer.valueOf(tmp[1]));
        }
        // recur all Step3Data to combine two
        List<Map.Entry<Integer, Integer>> list = new ArrayList<Map.Entry<Integer, Integer>>();
        for(Map.Entry<Integer, Integer> entry:map.entrySet()){
            int n = list.size();
            Map.Entry<Integer, Integer> user1 = entry;
            String pair;
            for(int i=0;i<n;i++){
                Map.Entry<Integer, Integer> user2 = list.get(i);
                int userId1 = user1.getKey(), userId2 = user2.getKey();
                if(userId1>userId2){
                    userPairs.set(userId2,userId1);
                }else{
                    userPairs.set(userId1,userId2);
                }
                int total = user1.getValue() + user2.getValue();
                union.set(total);
                context.write(userPairs, union);
            }
            list.add(user1);
        }
    }
}
