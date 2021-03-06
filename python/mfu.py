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
#quit()

comm  = flist.comm()
rank  = flist.rank()
ranks = flist.num_ranks()

flist.spread()
flist.sort('-size')
flist.write('test.txt', text=True)
#flist.read('test.mfu')
print(flist)
#quit()

print("Rank: ", rank, "Ranks: ", ranks, "Global size: ", flist.global_size(), "Offset: ", flist.global_offset(), "Local size: ", len(flist))

l = [1, 2, 3]
print(l[:-10])

if rank == 0:
  for f in flist[:]:
    print(rank, f.user, f.group, f.uid, f.gid, f.mode, f.size, f)
  for f in flist[1:]:
    print(rank, f.user, f.group, f.uid, f.gid, f.mode, f.size, f)
  for f in flist[:-2]:
    print(rank, f.user, f.group, f.uid, f.gid, f.mode, f.size, f)
  for f in flist[:-1]:
    print(rank, f.user, f.group, f.uid, f.gid, f.mode, f.size, f)
  for f in flist[-2:]:
    print(rank, f.user, f.group, f.uid, f.gid, f.mode, f.size, f)

flist.sort("-size")
if rank == 0:
  for f in flist[:3]:
    print(rank, f.user, f.group, f.uid, f.gid, f.mode, f.size, f)
  
flist.sort("size")
if rank == 0:
  for f in flist[:3]:
    print(rank, f.user, f.group, f.uid, f.gid, f.mode, f.size, f)

for f in flist:
  print(f.size, f)

flist2 = flist.subset()
for f in flist:
  if f.size < 10:
    flist2.append(f)
flist2.summarize()
print(rank, flist2)
for f in flist2:
  print(f.size, f)

# create a list of all items whose size is less than 10 bytes
flist3 = flist.subset(fn = lambda f: f.size < 10)
print(rank, flist3)
for f in flist3:
  print(f.size, f)

# create a list of all directories or items whose size is less than 10 bytes
flist4 = flist.subset(fn = lambda f: f.type == TYPE_DIR or f.size < 10)
print(rank, flist4)
for f in flist4:
  print(f.size, f)

# divide list into two, with first list containing all directories,
# second list is everything else
flist_dirs, flist_notdirs = flist.subset(fn = lambda f: f.type == TYPE_DIR, pivot=True)
print(rank, flist_dirs, flist_notdirs)
print("in")
for f in flist_dirs:
  print(f.size, f)
print("out")
for f in flist_notdirs:
  print(f.size, f)
