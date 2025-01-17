




dup:
	docker-compose up -d
ddown:
	docker-compose down

build_jobmanager_image:
	docker build -f Dockerfile.jobmanager --platform linux/amd64 -t spycloud_jobmanager .
