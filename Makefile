all:
	g++ -g2 trimtester.cpp -lboost_system -lboost_thread -o TrimTester

clean:
	rm -f *~ TrimTester
