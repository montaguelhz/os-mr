make clean
make 
START=`date +%s%N`;
find . -name "*.txt" | xargs ./mr > ./test-out/multi-out
END=`date +%s%N`;
time=$((END-START))
time=`expr $time / 1000000`
echo "cost time is $(($time))ms"
rm -f ../test-out/mr*
sort ./test-out/multi-out > ans
