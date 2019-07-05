# sampledataflow
Sample implementation of Google Cloud DataFlow pipeline based on Apache Beam SDK

# Overview
This sample pipeline does a word count from pub sub topic and writes it to GCS bucket.

Subscribe to pub sub topic -> Do a word count -> Write output to Gcs bucket

# Usage
Download/ clone project and import it as maven project

# Gcp credentials
Provide path to Gcp Credentials(Json file) in WindmillPipeline#main method

# Run

Run the following maven command to run the pipeline. Make sure to update project name, pub-sub topic name and gcs bucket before running.
```

mvn -Pdataflow-runner compile exec:java -Dexec.mainClass=WindmillPipeline "-Dexec.args=--project=cloud-dataflow-244206 --gcpTempLocation=gs://<BUCKET_NAME>/tmp --stagingLocation=gs://<BUCKET_NAME>/staging/ --pubsubTopic=newpubsub --output=gs://<BUCKET_NAME>/output/ --runner=DataflowRunner"

```


