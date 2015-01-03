package naivebayes;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Bingwei Liu
 *
 * Merge all documents with model 
 * Each line is one review. Two labels at the beginning of each line.
 * Label 1: Polarity={POS, NEG}
 * Label 2: id={000-999}
 * Labels are bounded with two colons ":"
 * Output each word and 1 followed by it's doc id and polarity label (POS or NEG)
 * An output from this mapper will contain "::", which is used to identify from model words
 */
public class NBTestPrepareMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {
        // value is one line in the input file, which is one review
    	// two labels at the begining of each line
    	// :POS:/:NEG:
    	// :id: the review id number
    	String valuestr = value.toString();
    	if (valuestr.contains(",")) { // this is a model line
    		String [] a = valuestr.split("\\s");
    		context.write(new Text(a[0]), new Text(a[1]));
    	} else 	if (valuestr.contains(":")) { // this is a review line
    	String[] doc = valuestr.split(" ");
//        String truelabel = doc[0];
//        String id = doc[1];
    	String truelabel = doc[0].replaceAll(":", "");
        String id = doc[1].replaceAll(":", "");
        
        for (int i = 2; i < doc.length; i++) { 
        	String word=doc[i].trim();
        	if (!word.isEmpty()){
//        		String outstr="1"+id+truelabel;
        		String outstr="1:"+id+truelabel;
        		context.write(new Text(word), new Text(outstr));
//        		context.write(new Text(word), new Text(outstr.replaceAll("::", ":")));
        	}
          }
        }
    }
}
