package org.apache.hadoop.mrunit.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class CustomWritable implements WritableComparable<CustomWritable> {
	private Text text = new Text();

	public CustomWritable() {
	}

	public CustomWritable(String text) {
		set(text);
	}

	public CustomWritable(Text text) {
		this.text = text;
	}

	public Text get() {
		return text;
	}

	public void set(String text) {
		this.text.set(text);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		text.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		text.readFields(in);
	}

	@Override
	public boolean equals(Object obj) {
		return CustomWritable.class.cast(obj).equals(this);
	}

	@Override
	public int hashCode() {
		return text.hashCode();
	}

	@Override
	public int compareTo(CustomWritable other) {
		return Integer.valueOf(this.hashCode()).compareTo(other.hashCode());
	}
}