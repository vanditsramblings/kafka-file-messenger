package com.rambler.messenger.receiver.listener;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class FileReceivedListener implements Runnable {
	private final KafkaConsumer<String, Map> consumer;
	private final int pollInterval;

	public FileReceivedListener(final KafkaConsumer<String, Map> consumer,int pollInterval) {
		this.consumer = consumer;
		this.pollInterval = pollInterval;
	}
	
	public void start(){
		System.out.println("File Received Listener started");
		Thread t=new Thread(this,"file-received-listener");
		t.start();
		
	}
	
	@Override
	public void run() {
		while (true) {
			ConsumerRecords<String, Map> records = consumer.poll(pollInterval);
			for (ConsumerRecord<String, Map> record : records) {
					Map<Object,Object> messageMap = (Map) record.value();
					System.out.println("#####################################################");
					System.out.println("Message Received : ID : "+record.key());
					if(messageMap!=null){
						for(Entry<Object, Object> entry:messageMap.entrySet()){
							String fileName=(String) entry.getKey();
							//byte[] fileArray = (byte[]) entry.getValue();
							System.out.println("\n\nReceived File Name :"+fileName);
							System.out.println("\n\nReceived File Content :\n\n"+entry.getValue()+"\n\n");
						}
						
					}
					System.out.println("#####################################################");
				
			}
		}
	}

}