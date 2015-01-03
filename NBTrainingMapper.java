package naivebayes;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Bingwei Liu
 *
 * Parse the input file. 
 * Each line is one review. Two labels at the beginning of each line.
 * Label 1: Polarity={POS, NEG}
 * Label 2: id={000-999}
 * Labels are bounded with two colons ":"
 * Output each word and 1 followed by it's polarity label (POS or NEG)
 */
public class NBTrainingMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {
        // value is one line in the input file, which is one review
    	// two labels at the begining of each line
    	// :POS:/:NEG:
    	// :id: not used in this case
    	String[] doc = value.toString().split(" ");
        String label = doc[0];
        
        for (int i = 2; i < doc.length; i++) { 
        	String word=doc[i].trim();
        	if (!word.isEmpty()){
        		context.write(new Text(word), new Text("1"+label));
        	}
        }
    }
}
