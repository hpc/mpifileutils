from mpifileutils import *

#flist = FList(read='test.mfu')
#flist.sort('size')
#flist.write('test.txt', text=True)
##print(flist)
#quit()

flist = FList()
flist.walk('../testdir')
flist.chmod(mode="g+w", group="tools")
flist.write('test.txt', text=True)
quit()

comm  = flist.comm()
rank  = flist.rank()
ranks = flist.num_ranks()

flist.spread()
flist.sort('-size')
flist.write('test.txt', text=True)
#flist.read('test.mfu')
print(flist)
quit()

print("Rank: ", rank, "Ranks: ", ranks, "Global size: ", flist.global_size(), "Offset: ", flist.global_offset(), "Local size: ", len(flist))

if rank == 0:
  for f in flist[:10]:
    print(rank, f.user, f.group, f.uid, f.gid, f.mode, f.size, f)

flist.sort("-size")
if rank == 0:
  for f in flist[:10]:
    print(rank, f.user, f.group, f.uid, f.gid, f.mode, f.size, f)
  
flist.sort("size")
if rank == 0:
  for f in flist[:10]:
    print(rank, f.user, f.group, f.uid, f.gid, f.mode, f.size, f)

flist2 = flist.subset()
for f in flist:
  if f.size < 100:
    #print(rank, f.size, f)
    flist2.append(f)
flist2.summarize()
print(rank, flist2.global_size())
for f in flist2[:10]:
  print(f)
