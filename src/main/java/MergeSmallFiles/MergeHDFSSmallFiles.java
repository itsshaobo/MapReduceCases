package MergeSmallFiles;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class MergeHDFSSmallFiles {

    public static class MergeMapper extends Mapper<NullWritable, BytesWritable, Text, BytesWritable> {

        private Text filenameKey ;

        @Override
        public void setup(Context context)
                throws IOException, InterruptedException {
            InputSplit split = context.getInputSplit();
            Path path =  ((FileSplit)split).getPath();
            filenameKey = new Text(path.toString());
        }

        @Override
        public void map(NullWritable key, BytesWritable value, Context context)
                throws IOException, InterruptedException{
            context.write(filenameKey, value);
        }
    }


    public static void main(String [] args) {

        //调用驱动程序

    }
}


class WholeFileInputFormat extends FileInputFormat<NullWritable, BytesWritable> {

    @Override
    public boolean isSplitable (JobContext context, Path file) {
        return false;
    }

    @Override
    public RecordReader<NullWritable, BytesWritable>
        createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException  {
        WholeFileRecordReader reader = new WholeFileRecordReader();
        reader.initialize(split, context);
        return reader;
    }
}

class WholeFileRecordReader extends RecordReader<NullWritable, BytesWritable> {
    //读取一个文件的 RecordReader

    private FileSplit fileSplit;
    private Configuration conf;
    private BytesWritable value = new BytesWritable();
    private boolean processed = false;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException{
        //通过分片对RecordReader初始化
        this.fileSplit = (FileSplit)split;
        this.conf = context.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        //将整个文件作为一个分片，产生一个Record，如果读过就将processed设置为true
        if(!processed) {
            //如果没有被处理，就打开一个流，将文件中的内容保存到bytearray中
            byte [] contents = new byte[(int)fileSplit.getLength()];
            Path file = fileSplit.getPath();
            FileSystem fs = file.getFileSystem(conf);
            FSDataInputStream in = null;
            try{
                in = fs.open(file);
                IOUtils.readFully(in, contents, 0, contents.length);
                value.set(contents,0, contents.length);
            }finally {
                IOUtils.closeStream(in);
            }
            processed = true;
            return true;
        }
        return false;
    }

    @Override
    public NullWritable getCurrentKey()
            throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public BytesWritable getCurrentValue() {
        return value;
    }

    @Override
    public float getProgress() throws IOException {
        return processed ? 1.0f : 2.0f;
    }

    @Override
    public void close() throws IOException {}

}










