## Kafka File Messenger

-This project demonstrates Custom Kafka serializer/deserializer and simulates sending and receiving of a 
 file through Kafka service

--------------------------

### Kafka Sender

- Sends a file through Kafka

Main Class :`KafkaFileSender`

>USAGE :  
> 
> -b|--broker-list   :Comma separated list of brokers  
  -t|--topic         :topic name  
  -f|--file          :absolute path of file to be sent  
--help print help"  

<b>Example</b> :

`java KafkaFileSender -b localhost:9091 -t fileTopic -f /home/Message.txt`
	
--------------------------

### Kafka Receiver

- Receives a file through kafka

Main Class :`KafkaFileReceiver`

>USAGE :   
  -b|--broker-list       :Comma separated list of brokers  
  -t|--topic             :topic name  
  -g|--group-id          :group id  
  -p|--poll-interval     :Poll interval[optional]  
  -c|--cleint-id         :client id [optional]  

<b>Example</b> :

`java KafkaFileReceiver -b localhost:9091 -t fileTopic -g group1 -p 1000`

--------------------------

Complete steps to run a simulation

1. Start Zookeeper server
2. Start Kafka Server
3. Create a topic
4. Start Receiver
5. Start Sender

--------------------------

Dependencies :

- kafka-clients-0.10.0.0.jar
- slf4j-api-1.7.21.jar

--------------------------



