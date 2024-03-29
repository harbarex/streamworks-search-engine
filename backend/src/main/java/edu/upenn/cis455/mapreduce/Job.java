package edu.upenn.cis455.mapreduce;

import java.util.ArrayList;
import java.util.Iterator;

public interface Job {

	// The map function (with optional field to add the node ID of the executor
	// to make debugging easier
	void map(String key, String value, Context context, String sourceExecutor);

	void map(String key, String[] values, Context context, String sourceExecutor);

	// The reduce function (with optional field to add the node ID of the executor
	// to make debugging easier
	void reduce(String key, Iterator<String> values, Context context, String sourceExecutor);

	void reduce(String key, String[] values, Context context, String sourceExecutor);

}
