# word-count-kafka
Count the Top 100 words in Moby Dick using the Confluent Platform.

## Requirements
1. Docker version 1.11 or later is installed and running.
2. Docker Compose is installed. It is installed by default with Docker for Mac and Windows.
3. Docker memory resource is allocated minimally at 8 GB.
4. Maven is installed.

### Starting the Demo
1. From the root directory, `docker-compose up -d`. The first time you run this statement, it may take Docker a while to download the necessary components and build the necessary images.  
   Ensure that all the services are up and operational by running `docker-compose ps`.
2. Navigate to Control Center web interface at: http://localhost:9021/  
   Note that it might take a couple minutes for the Control Center to come online. 
3. Run the moby-dick-producer project by stepping into the sub-directory and running `mvn clean install spring-boot:run`. As data is produced to the topic, the program will output the number of words produced.
4. From the Control Center, you may see the data being produced to the *wordCountInput* topic by navigating to **Topics** in the left pane and double-clicking the *wordCountInput* topic from the main pane. Navigate to the **INSPECT** tab to see data as it flows in.
5. Run the count-top-100 project by stepping into the sub-directory and running `mvn clean install spring-boot:run`. As data is produced to the topic, the program will output an Avro record containing a map to the *wordCountOutput* topic.
   
### Stopping the Demo
1. To shut down docker processes and disconnect the volumes (flush data), `docker-compose down -v`.
   
#### Helpful References
The following were used to create this quick-n'-dirty demo:
* https://docs.confluent.io/current/quickstart/ce-docker-quickstart.html
* https://www.baeldung.com/spring-kafka
* https://docs.spring.io/spring-boot/docs/current/reference/html/boot-features-messaging.html#boot-features-kafka