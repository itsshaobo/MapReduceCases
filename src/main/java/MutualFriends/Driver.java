package MutualFriends;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class Driver extends Configured implements Tool {

    public int run(String [] args)
            throws IOException,InterruptedException, ClassNotFoundException {
        if(args.length<4){
            System.err.println("wrong with the input and output file paths");
            return 1;
        }
        String input = args[0];
        String intermediate = args[1];
        String output = args[2];


        Configuration conf = getConf();
        conf.setBoolean("mapred.output.compress", true);
        if(runFirstStep(conf, input, intermediate)==0){
            conf.setBoolean("mapred.output.compress", false);
            return runSecondStep(conf, intermediate, output);
        }else{
            return 1;
        }
    }

    private int runFirstStep(Configuration conf, String input, String output)
            throws IOException,InterruptedException, ClassNotFoundException {
        Job pairJob = Job.getInstance(conf, "pairing friends");
        pairJob.setJarByClass(getClass());
        //mapper&reducer
        pairJob.setMapperClass(Step1.Mapper1.class);
        pairJob.setReducerClass(Step1.Reducer1.class);
        pairJob.setNumReduceTasks(100);
        //mapoutput key and value
        pairJob.setMapOutputKeyClass(Text.class);
        pairJob.setMapOutputValueClass(Text.class);
        //file paths
        FileInputFormat.addInputPath(pairJob, new Path(input));
        SequenceFileOutputFormat.setOutputPath(pairJob, new Path(output));
        //file format
        pairJob.setOutputFormatClass(SequenceFileOutputFormat.class);

        pairJob.setOutputKeyClass(Text.class);
        pairJob.setOutputValueClass(Text.class);

        return pairJob.waitForCompletion(true)?0:1;
    }

    private int runSecondStep(Configuration conf, String input, String output)
            throws IOException,InterruptedException, ClassNotFoundException {
        Job countPairJob = Job.getInstance(conf, "count pairs job");
        //set driver
        countPairJob.setJarByClass(Driver.class);
        //mapper&reducer
        countPairJob.setMapperClass(Step2.Mapper2.class);
        countPairJob.setNumReduceTasks(100);
        //mapoutput key and value
        countPairJob.setMapOutputKeyClass(Text.class);
        countPairJob.setMapOutputValueClass(Text.class);
        //file paths
        SequenceFileInputFormat.addInputPath(countPairJob, new Path(input));
        countPairJob.setInputFormatClass(SequenceFileInputFormat.class);
        SequenceFileOutputFormat.setOutputPath(countPairJob, new Path(output));
        //fileformat
        countPairJob.setOutputFormatClass(TextOutputFormat.class);
        countPairJob.setOutputKeyClass(Text.class);
        countPairJob.setOutputValueClass(Text.class);

        return countPairJob.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Driver(), args);
        System.exit(exitCode);
    }
}












