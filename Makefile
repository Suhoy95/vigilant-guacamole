
all:
	python3 -m venv venv
	venv/bin/pip install -r requirements.txt

	mkdir -p tmp
	echo "Hello, world!" > tmp/hello.txt
	dd if=/dev/urandom of=tmp/hundred.txt bs=1 count=100
	dd if=/dev/urandom of=tmp/meter.txt bs=1024 count=1024

	mkdir -p tmp/test/
	dd if=/dev/urandom of=tmp/test/hundred.txt bs=1 count=100