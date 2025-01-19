




dup:
	docker compose -f kafka-docker-composer.yaml  up -d
ddown:
	docker compose -f kafka-docker-composer.yaml  down

build_spycloud_flink:
	docker build -f Dockerfile.jobmanager --platform linux/amd64 -t spycloud_flink .

drun_producer:
	docker exec flink-jobmanager python /usr/local/workdir/python_src/flink_basics/flink_basics/flink_producer.py

