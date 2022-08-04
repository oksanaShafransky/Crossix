# Crossix - sort file with limited space
This project intends to sort efficiently a file with limited number of records in memory.
It takes input file, reads it in chunks according to the required size.
Each chunk of which is sorted independently of the next, and written to its own temporary file.
Afterwards, it reads lines from each temporary file into memory and merging the lines together in sorted order, 
writing out to the final output file moving along through each temporary file.

Time Complexity = O((CS log S) + (CS log C))
Space Complexity = O(C)

C - num of chunks
S - num of event in each chunk
CS - total num of entries in the input file (C*S) 





