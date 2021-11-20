build:; go build -o target/redashProxy *.go

build-linux:; export GOOS=linux && export GOARCH=amd64 && go build -o target/redashProxy *.go

clean:; rm -rf target/*

test:; go test -v

run:; ./target/redashProxy 

