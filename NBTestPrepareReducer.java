package com.ift.hadoop;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NBTestPrepareReducer  extends Reducer<Text, Text, Text, Text> {
  @Override
  public void reduce(Text key, Iterable<Text> values, Context context) 
	            throws InterruptedException, IOException {
	// each time this reducer is called, we have a new word
	// each value= 1:idPOS
	 
	  // count the number of occurences of this word for each review
    String modelstr="";
    HashMap<String, Integer> counts = new HashMap<String, Integer>();
    for (Text value : values) {
    	String v=value.toString();
    	if (v.contains(":")) { // this is a word from the test review 
				String[] review = v.split(":");
				int count = Integer.valueOf(review[0]);
//			    int count = Integer.valueOf(v.substring(0,v.indexOf(":")));
//			    String label = v.substring(v.indexOf(":"));
				String label = review[1];
			    counts.put(label, new Integer(counts.containsKey(label) ? counts.get(label).intValue()+ count : count));
			  } else {
					  modelstr = v+ " ";
				  }
	      }
		  
		  StringBuilder out = new StringBuilder();
		  if (!modelstr.isEmpty() && counts.size()>0) {
			  out.append(modelstr);
		  for (String label : counts.keySet()) {
			  out.append(String.format("%s:%s ", counts.get(label).intValue(),label));
		  }
			// Write out the results in the format:
			// <word> {<# of occurrences in POS>:<# of of occurrences in NEG>} 
	      context.write(key, new Text(out.toString().trim()));
		  }
//	  }
	}
}
 