package com.rambler.messenger.sender.serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;

public class KafkaMapSerializer implements Serializer<Map<?,?>> {

	@Override
	public void close() {

	}

	@Override
	public void configure(Map configs, boolean isKey) {
	}

	@Override
	public byte[] serialize(String topic, Map data) {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutput out = null;
		byte[] bytes = null;
		try {
			out = new ObjectOutputStream(bos);
			out.writeObject(data);
			bytes = bos.toByteArray();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (out != null) {
					out.close();
				}
			} catch (IOException ex) {
				// ignore close exception
			}
			try {
				bos.close();
			} catch (IOException ex) {
				// ignore close exception
			}
		}
		return bytes;
	}
}
