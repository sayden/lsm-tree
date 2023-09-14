# lsm-tree

1. non-existent folder
2. folder exists
3. find wal if any
4. find sst files
5. find index (file with info about the data inside every sst file)

if folder does not exists, create it, create a wal and an index file and return them.
if folder exists:
    if index file is present, load it, otherwise create one first
    if wal is present, maybe there was a failure, persist it, create a new wal and return it
    if wal is not present, create one and return it

After successful load:
    1 wal file
    1 index file that has data of 0..* sst files