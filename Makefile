all: clean build

clean:
	@echo "## rm overlit"
	@rm -f overlit

build:
	@echo "## build overlit"
	@go build -ldflags "-extldflags=-Wl,--allow-multiple-definition" .

style:
	@echo "## style overlit"
	@gofmt -w .

getall:
	@echo "## get all dependencies"
	@go get -d .
