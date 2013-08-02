#mpirun  -np 4 -machinefile ./machines hostname

rm *.log
rm test.tar

exe="./dtar  -c -f test.tar  3.txt  dtar.c copy.c 1.txt" 

mpirun -np 5  -machinefile ./machines $exe 

#mpirun -np 4 -machinefile ./machines  gdb ./dcp 




