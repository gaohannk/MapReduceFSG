
command line:

hadoop jar <jar file> <input directory> <output directory> <# iterations>  <# support>

I currently having it doing iterations because using hadoop 1.0.1.

So my suggestion is to first run it with a high number of iterations, find largest
subgraph size, and run again with largest+1.  So for example, since the largest subgraphs
we have are of size 3, put iterations as 4.

the support is actual number of graphs, NOT the percentage.

At each iteration (besides A1), we will output a directory A and B

outputMapRedAx
and
outputMapRedBx

results are in the file part-0000m  (m because there can be multiple files)

Ax holds ALL subgraphs constructed of size x, along with labels numbered and sorted alphabetically

Bx holds the subgraphs that meet the support (which is the information we want)