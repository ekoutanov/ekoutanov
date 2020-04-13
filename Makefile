default: build 

build: dirs
	go build -o bin ./...

dirs:
	mkdir -p bin

clean:
	rm -rf bin
