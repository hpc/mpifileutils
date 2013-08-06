#mpirun  -np 4 -machinefile ./machines hostname

rm ../*.log
rm test.tar

exe="./dtar  -c -f test.tar  ../treewalk.c  ../dtar.c  ../copy.c ../README  ./dir" 

#exe="./dtar  -c -f test.tar  ../treewalk.c  ../dtar.c  ../copy.c ../README " 
mpirun -np 5  -machinefile ./machines $exe 

#mpirun -np 4 -machinefile ./machines  gdb ./dcp 




