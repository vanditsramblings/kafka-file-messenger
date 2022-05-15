package com.rambler.messenger.sender;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaFileSender {

	private String brokerList="";
	private String topic="";
	private String file="";
	private Properties senderProps=new Properties();
	private Producer<String, Map> producer;

	// Serializers/Deserializers
	public static final String STRING_SERIALIZER_URI = "org.apache.kafka.common.serialization.StringSerializer";
	public static final String MAP_SERIALIZER_URI = "com.rambler.messenger.sender.serializer.KafkaMapSerializer";
	public static int BUFFER=2048;

	public String usage="Usage :\n"
			+ "-b|--broker-list :Comma separated list of brokers\n"
			+ "-t|--topic :topic name \n"
			+ "-f|--file :absolute path of file to be sent \n"
			+ "--help print help";

	public boolean parseArgs(String[] args) {

		if(args.length<6){
			printUsage();
			return false;
		}

		for(int i=0;i<args.length;i++){
			if(args[i].equals("-b")||args[i].equals("--broker-list")){
				if(!args[i+1].isEmpty() && !args[i+1].startsWith("-"))
					brokerList=args[i+1];
				else{
					printError("[-b]||[--broker-list]");
					return false;
				}
			}
			if(args[i].equals("-t")||args[i].equals("--topic")){
				if(!args[i+1].isEmpty() && !args[i+1].startsWith("-"))
					topic=args[i+1];
				else{
					printError("[-t]||[--topic]");
					return false;
				}
			}
			if(args[i].equals("-f")||args[i].equals("--file")){
				if(!args[i+1].isEmpty() &&  !args[i+1].startsWith("-"))
					file=args[i+1];
				else{
					printError("[-f]||[--file]");
					return false;
				}
			}
			if(args[i].equals("--help")){
				printUsage();
				return false;
			}
		}
		return true;
	}


	public static void main(String args[]){

		KafkaFileSender sender=new KafkaFileSender();
		boolean result=false;
		if(sender.parseArgs(args)){
			
			sender.printArgs();
			
			sender.init();

			sender.send();
		}
	}
	
	private void printArgs() {
		System.out.println("#########################################################");
		System.out.println("Starting Sender with following arguments : ");
		System.out.println("BROKER LIST    : "+brokerList);
		System.out.println("TOPIC          : "+topic);
		System.out.println("FILE	       : "+file);
		System.out.println("#########################################################");
	}

	private void send() {
		byte[] fileArray=getFileAsBytes(file);
		File fileToTransfer=new File(file);
		if(fileArray!=null){
			HashMap<String,byte[]> messageMap=new HashMap<String,byte[]>();
			messageMap.put(fileToTransfer.getName(), fileArray);
			
			System.out.println("Sending file : "+file);
			final Object message = new ProducerRecord<String, Map>(topic,Integer.toString(getMessageId()), messageMap);
			producer.send((ProducerRecord<String, Map>) message);
			System.out.println("File Sent");
		}
	}

	private int getMessageId() {
		Random randomGenerator = new Random();
		return randomGenerator.nextInt(100);
	}


	private byte[] getFileAsBytes(String file) {
		File fileToTransfer=new File(file);

		if(!fileToTransfer.exists()){
			System.out.println("File does not exist.Aborting.");
			return null;
		}
		FileInputStream inputStream;
		try {
			inputStream = new FileInputStream(fileToTransfer);
		} catch (FileNotFoundException e) {
			System.out.println("File does not exist.Aborting.");
			e.printStackTrace();
			return null;
		}
		byte[] b = new byte[BUFFER];
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		int c;
		try {
			while ((c = inputStream.read(b)) != -1) {
				os.write(b, 0, c);
			}
		} catch (IOException e) {
			System.out.println("Unable to read file.Aborting.");
			e.printStackTrace();
			return null;
		}
		return os.toByteArray();
	}


	public void init(){

		//Initializing properties

		final Properties props = new Properties();
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, STRING_SERIALIZER_URI);	
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MAP_SERIALIZER_URI);
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerList);
		senderProps = props;

		//Intializing KafkaProducer
		producer = new KafkaProducer<>(senderProps);

	}

	public void printUsage(){
		System.out.println(usage);
	}

	public void printError(String argument){
		System.out.println("Missing mandatory argument :"+argument );
	}


}
