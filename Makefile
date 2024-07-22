.PHONY: build deploy

build:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -buildvcs=false -o=bin/my-noderesourcesfit ./cmd/scheduler

buildLocal:
	docker build . -t mike911209/my-noderesourcesfit:latest
	docker push mike911209/my-noderesourcesfit:latest

loadImage:
	kind load docker-image my-scheduler:local

deploy:
	helm install noderesourcesfit charts/ --create-namespace --namespace scheduler-plugins

remove:
	helm uninstall -n scheduler-plugins noderesourcesfit

clean:
	rm -rf bin/
