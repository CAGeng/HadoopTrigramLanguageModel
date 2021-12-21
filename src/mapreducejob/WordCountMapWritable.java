package mapreducejob;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;

public class WordCountMapWritable extends MapWritable {

    public void addCount(Text key, int count){
        if(!this.containsKey(key)) this.put(key, new IntWritable(count));
        else this.put(key, new IntWritable(((IntWritable) this.get(key)).get() + count));
    }

}
