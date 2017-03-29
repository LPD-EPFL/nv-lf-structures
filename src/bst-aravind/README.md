# lfbst
A Lock-Free Unbalanced Binary Search Tree

Citation:
@inproceedings{Natarajan:2014:FCL:2555243.2555256,
 author = {Natarajan, Aravind and Mittal, Neeraj},
 title = {Fast Concurrent Lock-free Binary Search Trees},
 booktitle = {Proceedings of the 19th ACM SIGPLAN Symposium on Principles and Practice of Parallel Programming},
 series = {PPoPP '14},
 year = {2014},
 isbn = {978-1-4503-2656-8},
 location = {Orlando, Florida, USA},
 pages = {317--328},
 numpages = {12},
 url = {http://doi.acm.org/10.1145/2555243.2555256},
 doi = {10.1145/2555243.2555256},
 acmid = {2555256},
 publisher = {ACM},
 address = {New York, NY, USA},
 keywords = {binary search tree, concurrent data structure, lock-free algorithm},
} 

To compile: make
To run: ./wfrbt <command line arguments>

The code uses TCMALLOC and the atomic_ops library. You can build a version that uses regular malloc by updating VERSION in the makefile to 'MALLOC_NO_UPDATE'.

The command line arguments of interest are:
-k : maximum number of keys in the tree (key space size)
-n: number of threads
-d: duration of test in milliseconds
-i : insert fraction (number between 0 and 1)
-x : delete fraction (number between 0 and 1)
-r : search fraction (number between 0 and 1)
Please ensure that the sum of the insert, delete and search fractions is 1.

