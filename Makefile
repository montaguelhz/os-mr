all:
	gcc wc.c mapreduce.c -o mr -pthread

clean:
	rm -f mr
	rm -f ./test-out/*