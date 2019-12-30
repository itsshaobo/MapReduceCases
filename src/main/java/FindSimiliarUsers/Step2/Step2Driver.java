package FindSimiliarUsers.Step2;

import FindSimiliarUsers.MyPair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class Step2Driver extends Configured implements Tool {

    public int run(String [] args)
            throws IOException,InterruptedException, ClassNotFoundException {
        if(args.length<6){
            System.err.println("wrong with the input and output file paths");
            return 1;
        }
        String input = args[0];
        String interaction1 = args[1];
        String interaction2 = args[2]; // used to merge
        String union1 = args[3];
        String union2 = args[4];      // used to merge
        String res = args[5];

        Configuration conf = new Configuration();


        int flag = 1;
        if(runFirstStep(conf, input, interaction1)==0){
            runSecondStep(conf, interaction1, interaction2);
            flag = 0;
        }

        if(flag == 0 && runThirdStep(conf, input, union1)==0 && runForthStep(conf, union1, union2)==0){
            return  joinTowStep1(conf, union2, interaction2, res);
        }
        return 1;
    }

    private int runFirstStep(Configuration conf, String input, String output)
            throws IOException,InterruptedException, ClassNotFoundException {
        Job interactionJob1 = Job.getInstance(conf, "interaction job step 1");

        interactionJob1.setJarByClass(getClass());
        //mapper&reducer
        interactionJob1.setMapperClass(InteractionMapper1.class);
        interactionJob1.setReducerClass(InteractionReducer1.class);
        //mapoutput key and value
        interactionJob1.setMapOutputKeyClass(IntWritable.class);
        interactionJob1.setMapOutputValueClass(IntWritable.class);
        interactionJob1.setNumReduceTasks(100);
        //file paths
        FileInputFormat.addInputPath(interactionJob1, new Path(input));
        SequenceFileOutputFormat.setOutputPath(interactionJob1, new Path(output));
        //file format
        interactionJob1.setOutputFormatClass(SequenceFileOutputFormat.class);

        interactionJob1.setOutputKeyClass(MyPair.class);
        interactionJob1.setOutputValueClass(IntWritable.class);
        return interactionJob1.waitForCompletion(true)?0:1;
    }

    private int runSecondStep(Configuration conf, String input, String output)
            throws IOException,InterruptedException, ClassNotFoundException {
        Job interactionJob2 = Job.getInstance(conf, "interaction job step 2");
        //set driver
        interactionJob2.setJarByClass(Step2Driver.class);
        //mapper&reducer
        interactionJob2.setMapperClass(InteractionMapper2.class);
        interactionJob2.setReducerClass(InteractionReducer2.class);
        interactionJob2.setNumReduceTasks(100);
        //mapoutput key and value
        interactionJob2.setMapOutputKeyClass(MyPair.class);
        interactionJob2.setMapOutputValueClass(IntWritable.class);
        //input and output format and class
        SequenceFileInputFormat.addInputPath(interactionJob2, new Path(input));
        interactionJob2.setInputFormatClass(SequenceFileInputFormat.class);
        //FileOutputFormat.setOutputPath(interactionJob2, new Path(output));
        SequenceFileOutputFormat.setOutputPath(interactionJob2, new Path(output));
        interactionJob2.setOutputFormatClass(SequenceFileOutputFormat.class);

        interactionJob2.setOutputKeyClass(MyPair.class);
        interactionJob2.setOutputValueClass(IntWritable.class);

        return interactionJob2.waitForCompletion(true)?0:1;
    }

    private int runThirdStep(Configuration conf, String input, String output)
            throws IOException,InterruptedException, ClassNotFoundException {
        Job unionJob1 = Job.getInstance(conf, "union job step 1");

        unionJob1.setJarByClass(Step2Driver.class);
        //mapper&reducer
        unionJob1.setMapperClass(UnionMapper1.class);
        unionJob1.setReducerClass(UnionReducer1.class);
        //mapoutput Key and value
        unionJob1.setMapOutputKeyClass(IntWritable.class);
        unionJob1.setMapOutputValueClass(IntWritable.class);
        unionJob1.setNumReduceTasks(100);
        //file paths
        FileInputFormat.addInputPath(unionJob1, new Path(input));
        SequenceFileOutputFormat.setOutputPath(unionJob1, new Path(output));
        unionJob1.setOutputFormatClass(SequenceFileOutputFormat.class);
        //fileformat
        unionJob1.setOutputKeyClass(IntWritable.class);
        unionJob1.setOutputValueClass(IntWritable.class);
        return unionJob1.waitForCompletion(true)?0:1;

    }

    private int runForthStep(Configuration conf, String input, String output)
            throws IOException,InterruptedException, ClassNotFoundException {
        Job unionJob2 = Job.getInstance(conf, "interaction job step 2");
        //set driver
        unionJob2.setJarByClass(Step2Driver.class);
        //mapper&reducer
        unionJob2.setMapperClass(UnionMapper2.class);
        unionJob2.setReducerClass(UnionReducer2.class);
        unionJob2.setNumReduceTasks(100);
        //mapoutput key and value
        unionJob2.setMapOutputKeyClass(IntWritable.class);
        unionJob2.setMapOutputValueClass(Text.class);
        //input and output format and class
        SequenceFileInputFormat.addInputPath(unionJob2, new Path(input));
        unionJob2.setInputFormatClass(SequenceFileInputFormat.class);
        SequenceFileOutputFormat.setOutputPath(unionJob2, new Path(output));
        unionJob2.setOutputFormatClass(SequenceFileOutputFormat.class);

        unionJob2.setOutputKeyClass(MyPair.class);
        unionJob2.setOutputValueClass(IntWritable.class);

        return unionJob2.waitForCompletion(true)?0:1;
    }

    private int joinTowStep1(Configuration conf, String input1, String input2, String output)
            throws IOException,InterruptedException, ClassNotFoundException {
        /**
         * input1 repersents union Step3Data
         * input2 repersents interaction Step3Data
         * */
        Job joinJob = Job.getInstance(conf, "merge two kinds of Step3Data");
        //set driver
        joinJob.setJarByClass(Step2Driver.class);
        //multiple inputs
        MultipleInputs.addInputPath(joinJob, new Path(input1), SequenceFileInputFormat.class, TagUnionMapper.class);
        MultipleInputs.addInputPath(joinJob, new Path(input2), SequenceFileInputFormat.class, TagInterMapper.class);
        joinJob.setReducerClass(DenoReducer.class);
        //mapoutput key and value
        joinJob.setMapOutputKeyClass(MyPair.class);
        joinJob.setMapOutputValueClass(Text.class);
        joinJob.setNumReduceTasks(100);
        FileOutputFormat.setOutputPath(joinJob, new Path(output));
        joinJob.setOutputKeyClass(MyPair.class);
        joinJob.setOutputValueClass(IntWritable.class);
        return joinJob.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure(); // config log4j
        int exitCode = ToolRunner.run(new Step2Driver(), args);
        System.exit(exitCode);
    }
}