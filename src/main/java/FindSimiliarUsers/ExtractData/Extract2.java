package FindSimiliarUsers.ExtractData;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class Extract2{

    public static class Extract2Mapper extends Mapper<LongWritable, Text, IntWritable, Text > {

        private IntWritable userId = new IntWritable();
        private Text pair = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String [] tmp = value.toString().split("\t");
            int user = Integer.valueOf(tmp[0]);
            userId.set(user);
            pair.set(tmp[1]);
            context.write(userId, pair);
        }
    }

    public static class Extract2Reducer extends Reducer<IntWritable, Text, IntWritable, Text> {

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            if(key.get()%100==56){
                for(Text val:values){
                    context.write(key, val);
                }
            }
        }
    }

    public static void main(String [] args) throws Exception {
        BasicConfigurator.configure();
        Configuration conf = new Configuration();
        conf.set("mapreduce.output.key.field.separator", ":");
        Job job = Job.getInstance(conf, "word count");

        job.setJarByClass(Extract2.class);

        job.setMapperClass(Extract2Mapper.class);
        job.setReducerClass(Extract2Reducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}




