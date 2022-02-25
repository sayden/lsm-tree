## SST File
An SST file contains the following data:
 ______________________ 
|                      |
|        Header        |
|______________________|
 ______________________ 
|                      |
|      Data List       |
|______________________|
 ______________________
|                      |
|    Pointers list     |
|______________________|

The header contains critical information about the contents of the file. The data chunk contains full records in ascending order. The pointers list contains the same info than the records but replacing the value with offsets in the file. This effectively stored every operation and key twice: once in the data list and once in the pointers list.

### Header

The header contains information critical to use the file effectively. 

 __________________________________________________________________________________________
| 1 byte       | 8 bytes          | 8 bytes         | 128 bytes            | 8 bytes       | Total 33 bytes
|              |                  |                 |                      |               |
| Magic number | First key offset | Last key offset | Reserved Space       | Total records |
|______________|__________________|_________________|______________________|_______________|

* Magic number: Is a 1 byte to store whatever comes to your mind
* First key offset: The offset in bytes on the file of the beginning of the pointers list
* Last key offset: The offset in bytes on the file of the last pointer in the file.
* Reserverd space: 8 Bytes for future data
* Total records: How many records are in this file

### The Data list

It contains a list of records

#### Record
 ___________________________________________________________
| 1 byte    | 8 bytes    | X Bytes | 8 bytes      | X Bytes |
| Operation | Key length | Key     | Value length | Value   |
|-----------|------------|---------|--------------|---------|

### The pointer list
 ________________________________________________
| 1 byte    | 8 bytes    | X bytes | 8 bytes     |
| Operation | Key length | Key     | Byte offset |
|-----------|------------|---------|-------------|
