# FOMBook

This project is a Social Media Network for Students and Alumni of the [FOM University](https://www.fom.de/) built using Flask with MongoDB and Docker.
Using DigitalOcean Droplets, Load Balancers as well as managed MongoDB Database Clusters this application has the capability to scale up to thousands of concurrent users.

## Prerequisites

Before you begin, ensure you have the following installed on your system:

-   [Docker](https://docs.docker.com/get-docker/)
-   [Docker Compose](https://docs.docker.com/compose/install/)
-   [Git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)

## Getting Started

Follow these instructions to get the project up and running on your local machine.

### Clone the Repository

First, clone the repository to your local machine:

```sh
git clone git@github.com:robin-rpr/fombook.git
cd fombook
```

### Project Structure

The project directory contains the following files:

-   `Dockerfile`: Dockerfile for building the Flask application image.
-   `docker-compose.yml`: Docker Compose file to set up the Flask and MongoDB services (for development).
-   `requirements.txt`: List of Python dependencies for the Flask application.
-   `app.py`: The Flask application code.
-   `init-mongo.js`: JavaScript initialization script for MongoDB.

### Build and Start the Services

To build and start the Flask and MongoDB services, run the following command in the project directory:

```sh
docker-compose up
```

This command will:

1. Build the Docker image for the Flask application.
2. Start the MongoDB and Flask services.
3. Initialize MongoDB with the specified database and collection.

### Accessing the Application

Once the services are up and running, you can access the Flask application at [`http://localhost:8080`](http://localhost:8080).

# Acknowledgments

-   https://github.com/jenizar/flask-mongodb-crud
-   https://www.codeproject.com/Articles/1255416/Simple-Python-Flask-Program-with-MongoDB
-   https://stackoverflow.com/questions/45732838/authentication-failed-to-connect-to-mongodb-using-pymongo
-   https://stackoverflow.com/questions/40346767/pymongo-auth-failed-in-python-script

## License

```
/**
 * Copyright (C) FOMBook.com - All Rights Reserved
 * 
 * This source code is protected under international copyright law. All rights
 * reserved and protected by the copyright holders.
 * This file is confidential and only available to authorized individuals with the
 * permission of the copyright holders.  If you encounter this file and do not have
 * permission, please contact the copyright holders and delete this file.
 */
```
