build:; go build -o target/redashProxy *.go

build-linux:; export GOOS=linux && export GOARCH=amd64 && go build -o target/redashProxy *.go

clean:; rm -rf target/*

test:; go test -v

run:; ./target/redashProxy 

deploy:; scp    ./target/redashProxy  root@cdn_17: && scp ./target/deploy.sh root@cdn_17: && ssh root@cdn_17 'bash /root/deploy.sh'

