package FindSimiliarUsers.Step3;

import FindSimiliarUsers.MyPair;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.PriorityQueue;
import java.util.Stack;

public class FindSimiMapper extends Mapper<MyPair, FloatWritable, IntWritable, Text> {

    private IntWritable userId = new IntWritable();
    private Text simi = new Text();

    public void map(MyPair key, FloatWritable value, Context context)
            throws IOException, InterruptedException {
        userId.set(key.getUser1().get());
        simi.set(key.getUser2()+","+value);
        context.write(userId, simi);

        userId.set(key.getUser2().get());
        simi.set(key.getUser1() + "," + value);
        context.write(userId, simi);
        }
}

class FindSimiReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    /**
     * 采用堆排序
     *
     * */
    private static class UserSimi implements Comparable<UserSimi>{
        private int userId;
        private float similarity;

        public UserSimi(int user, float simi) {
            userId = user;
            similarity = simi;
        }

        public int getUser(){
            return userId;
        }

        public float getSimilarity(){
            return similarity;
        }

        @Override
        public int compareTo(UserSimi us){
            if(similarity>us.similarity){
                return 1;
            }else if(similarity>=us.similarity){
                return 0;
            }else{
                return -1;
            }
        }
    }
    private PriorityQueue<UserSimi> heap = new PriorityQueue<UserSimi>();
    private Text friends = new Text();

    public void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for(Text val:values){
            String [] tmp = val.toString().split(",");
            UserSimi us = new UserSimi(Integer.valueOf(tmp[0]), Float.valueOf(tmp[1]));
            if(heap.size()<3){
                heap.add(us);
            }else{
                if(us.getSimilarity()>heap.peek().getSimilarity()){
                    heap.poll();
                    heap.offer(us);
                }
            }
        }
        Stack<Integer> stack = new Stack<Integer>();
        while(!heap.isEmpty()){
            stack.push(heap.poll().getUser());
        }
        StringBuilder sb = new StringBuilder();
        while(!stack.isEmpty()){
            sb.append(stack.pop());
            sb.append(",");
        }
        sb.deleteCharAt(sb.length()-1);
        friends.set(sb.toString());
        context.write(key, friends);
    }
}
