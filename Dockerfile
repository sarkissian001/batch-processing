# Use the official OpenJDK image with JRE installed. This is needed since Pyspark Requires JAVA to be installed
FROM openjdk:11-jre-slim

WORKDIR /app

COPY requirements.txt .

RUN apt-get update && apt-get install -y python3 python3-pip && apt-get clean

RUN pip3 install --upgrade pip
RUN pip3 install -r requirements.txt

# Copy the rest of the application code into the image.
COPY . .

# Expose the port Dagit will run on.
EXPOSE 3000

# Set the default command to run Dagit with the specified pipeline file.
CMD ["dagit", "-f", "app/pipeline.py", "-h", "0.0.0.0", "-p", "3000"]
