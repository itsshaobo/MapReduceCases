package FindSimiliarUsers;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MovieWithScore implements WritableComparable<MovieWithScore> {
    private IntWritable movie;
    private FloatWritable score;

    public MovieWithScore(){
        movie = new IntWritable();
        score = new FloatWritable();
    }

    public MovieWithScore(int mov, float sco){
        movie = new IntWritable(mov);
        score = new FloatWritable(sco);
    }

    public void set(int mov, float sco){
        movie.set(mov);
        score.set(sco);
    }

    public IntWritable getMovie(){
        return movie;
    }

    public FloatWritable getScore(){
        return score;
    }

    @Override
    public void write(DataOutput dout) throws IOException {
        movie.write(dout);
        score.write(dout);
    }

    @Override
    public void readFields(DataInput din) throws IOException {
        movie.readFields(din);
        score.readFields(din);
    }

    @Override
    public int compareTo(MovieWithScore mws){
        int tmp = movie.compareTo(mws.movie);
        if(tmp==0) return score.compareTo(mws.score);
        return tmp;
    }

    @Override
    public String toString(){
        return movie + "\t" + score;
    }

    @Override
    public boolean equals(Object obj){
        if(obj instanceof MovieWithScore){
            MovieWithScore tmp = (MovieWithScore) obj;
            if(tmp.movie.equals(movie) && tmp.score.equals(score))
                return true;
        }
        return false;
    }

    @Override
    public int hashCode(){
        return movie.hashCode();
    }
}
