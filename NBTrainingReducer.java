package naivebayes;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NBTrainingReducer  extends Reducer<Text, Text, Text, Text> {
  @Override
  public void reduce(Text key, Iterable<Text> values, Context context) 
	            throws InterruptedException, IOException {
	// each time this reducer is called, we have a new word
	// this word could be in positive or in negative or both
    int pos_count=0;
    int neg_count=0;
    String outkey = key.toString().replaceAll("\\s+", "");
 
	  for (Text value : values) {
	  String[] v=value.toString().split(":");
      if (v[1].contains("POS")) {
    	  context.getCounter(NBController.NB_COUNTERS.TOTAL_WORDS_POS).increment(1);
    	  pos_count +=Integer.valueOf(v[0]);
    	  }
      if (v[1].contains("NEG")) {
    	  context.getCounter(NBController.NB_COUNTERS.TOTAL_WORDS_NEG).increment(1);
    	  neg_count +=Integer.valueOf(v[0]);
    	  }
      }
	  // increment the total number of occurrence in the union vacabulary of pos and neg
	  context.getCounter(NBController.NB_COUNTERS.VOCABULARY_SIZE_UNION).increment(1);
      if (pos_count*neg_count > 0) {
    	  // this word is a common word in pos and neg
    	  context.getCounter(NBController.NB_COUNTERS.VOCABULARY_SIZE_POS).increment(1);
    	  context.getCounter(NBController.NB_COUNTERS.VOCABULARY_SIZE_NEG).increment(1);
    	  } else if (pos_count>0) {
    		  context.getCounter(NBController.NB_COUNTERS.VOCABULARY_SIZE_POS).increment(1);
    		  } else {
    			  context.getCounter(NBController.NB_COUNTERS.VOCABULARY_SIZE_NEG).increment(1); 			  
		      }
		        // Write out the results in the format:
		        // <word> {<# of occurrences in POS>:<# of of occurrences in NEG>} 
      context.write(new Text(outkey), new Text(String.format("%s,%s", pos_count, neg_count)));
//      }
	}
}
 
