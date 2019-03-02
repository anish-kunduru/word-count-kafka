# word-count-kafka
Count the Top 100 words in Moby Dick using the Confluent Platform.

## Requirements
1. Docker version 1.11 or later is installed and running.
2. Docker Compose is installed. It is installed by default with Docker for Mac and Windows.
3. Docker memory resource is allocated minimally at 8 GB.

## Starting the Demo
1. From the root directory, `docker-compose up -d`. The first time you run this statement, it may take Docker a while to download the necessary components and build the necessary images.  
   Ensure that all the services are up and operational by running `docker-compose ps`.
2. Navigate to Control Center web interface at: http://localhost:9021/  
   Note that it might take a couple minutes for the Control Center to come online. 