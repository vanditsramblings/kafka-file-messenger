package com.rambler.messenger.receiver;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.rambler.messenger.receiver.listener.FileReceivedListener;

public class KafkaFileReceiver {
	private String brokerList="";
	private String topic="";
	private String groupId="kafkagroup";
	private String clientId="client1";
	private Properties receiverProps=new Properties();
	private KafkaConsumer<String, Map> receiver;
	private FileReceivedListener listener;
	private int pollInterval=100; //default
	
	public static int BUFFER=2048;
	
	// Serializers/Deserializers
	public static final String STRING_DESERIALIZER_URI = "org.apache.kafka.common.serialization.StringDeserializer";		
	public static final String MAP_DESERIALIZER_URI = "com.rambler.messenger.receiver.deserializer.KafkaMapDeserializer";


	
	

	public String usage="Usage :\n"
			+ "-b|--broker-list :Comma separated list of brokers\n"
			+ "-t|--topic :topic name \n"
			+ "-g|--group-id :group id \n"
			+ "-p|--poll-interval : Poll interval\n[optional]"
			+ "-c|--cleint-id : client id [optional]\n"
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
			if(args[i].equals("-g")||args[i].equals("--group-id")){
				if(!args[i+1].isEmpty() &&  !args[i+1].startsWith("-"))
					groupId=args[i+1];
				else{
					printError("[-g]||[--group-id]");
					return false;
				}
			}
			if(args[i].equals("-c")||args[i].equals("--client-id")){
				if(!args[i+1].isEmpty() &&  !args[i+1].startsWith("-"))
					clientId=args[i+1];
			}
			if(args[i].equals("-p")||args[i].equals("--poll-interval")){
				if(!args[i+1].isEmpty() &&  !args[i+1].startsWith("-"))
					pollInterval=Integer.parseInt(args[i+1]);
			}
			if(args[i].equals("--help")){
				printUsage();
				return false;
			}
		}
		return true;
	}


	public static void main(String args[]){

		KafkaFileReceiver receiver=new KafkaFileReceiver();
		boolean result=false;
		if(receiver.parseArgs(args)){
			receiver.printArgs();
			receiver.init();

			receiver.startListener();
		}
	}

	private void printArgs() {
		System.out.println("#########################################################");
		System.out.println("Starting Receiver with following arguments : ");
		System.out.println("BROKER LIST    : "+brokerList);
		System.out.println("TOPIC          : "+topic);
		System.out.println("GROUP ID       : "+groupId);
		System.out.println("CLIENT ID      : "+clientId);
		System.out.println("POLL INTERVAL  : "+pollInterval);
		System.out.println("#########################################################");
	}


	public void startListener(){
		listener.start();
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
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerList);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
		props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, STRING_DESERIALIZER_URI);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MAP_DESERIALIZER_URI);
		receiverProps = props;

		//Intializing KafkaConsumer
		receiver = new KafkaConsumer<String, Map>(receiverProps);
		receiver.subscribe(Arrays.asList(topic));
		
		listener=new FileReceivedListener(receiver,pollInterval); 
		

	}

	public void printUsage(){
		System.out.println(usage);
	}

	public void printError(String argument){
		System.out.println("Missing mandatory argument :"+argument );
	}


}
