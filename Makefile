all: clean build

clean:
	@echo "## rm overlit"
	@rm -f overlit

build:
	@echo "## build overlit"
	@go build -ldflags "-extldflags=-Wl,--allow-multiple-definition" .

ensure:
	@echo "## ensure dep"
	@dep ensure
