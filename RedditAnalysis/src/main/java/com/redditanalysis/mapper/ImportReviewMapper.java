package com.redditanalysis.mapper;

import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.redditanalysis.parser.XMLParser;
import com.redditanalysis.review.BadWords;
import com.redditanalysis.review.util.WordUtil;

public class ImportReviewMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	private static final String DELIMITER = ",";

	protected void map(
			LongWritable key,
			Text value,
		    Context context)
			throws java.io.IOException, InterruptedException {

		XMLParser xP;
		try {
			xP = new XMLParser(value.toString());

			List<String> categories = xP.getCategory();
			List<String> reviewLists = xP.getReviews();
			int postive = 0;
			int negative = 0;

			for (String eachReview : reviewLists) {
				if (BadWords.isBad(eachReview)) {
					negative++;
				} else {
					postive++;
				}
			}

			for (String eachCat : categories) {
				String mapOutput = WordUtil.cleanWords(eachCat) + DELIMITER + xP.getHash() + DELIMITER + xP.getUrl() + DELIMITER + postive + DELIMITER + negative + DELIMITER + xP.getUsercount();
				context.write(new Text(mapOutput),NullWritable.get());
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("");
		}


	};

}
