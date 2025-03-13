# Stream ingestion

This part contains the stream ingestion components of the platform.

* messaging system: infrastructure for messaging to and inside the platform
* pipelines: data ingestion, processing and storage insertion pipelines
* stream monitor: gets reports of pipeline executions, based on them calls the manager if needed
* stream manager: invokes the pipelines to start/stop
