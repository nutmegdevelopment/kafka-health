# Go parameters
GOCMD=go
GOMODULE=GO111MODULE=on
GOBUILD=$(GO111MODULE) $(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test

# General parameters
BINARY_NAME=kafka-health
REGISTRY=registry.nutmeg.co.uk:8443/
IMAGE_NAME=kafka-health
VERSION=${RELEASE_NUMBER}

build:
	$(GOMODULE) CGO_ENABLED=0 GOOS=linux $(GOBUILD) -o $(BINARY_NAME) -v
test: 
	$(GOTEST) -v
clean: 
	$(GOCLEAN)
run:
	$(GOBUILD) -v
	./$(BINARY_NAME)
docker:
	docker build -t $(REGISTRY)$(BINARY_NAME):$(VERSION) .
	docker push $(REGISTRY)$(BINARY_NAME):$(VERSION)
docker-run:
	docker run --rm -t -e KAFKA_URL=${KAFKA_URL} -e KAFKA_TOPIC=${KAFKA_TOPIC} 