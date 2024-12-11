# 定义目标
# lab2
.PHONY: stress-test compile-code unit-test clear compile
compile-code:
	mkdir -p build && cd build && cmake .. && make -j && make build-tests -j

unit-test:
	mkdir -p build && cd build && cmake .. && make -j && make build-tests -j && make test -j

stress-test:
	mkdir -p build && cd build && cmake .. && make -j && make build-tests -j && make run_concurrent_stress_test

clean:
	rm -f /tmp/inode_file /tmp/test_file1 /tmp/test_file2 /tmp/test_file3 /tmp/test_file 

compile:
	mkdir -p build && cd build && cmake -DCMAKE_EXPORT_COMPILE_COMMANDS=ON ..