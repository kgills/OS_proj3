all: KooToueg.java
	javac -g $^

clean:
	rm -rf *.class
	