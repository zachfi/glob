# Glob

A file replication tool.

## File operations

At the lowest level of file operations is the chunking. Before a file can be
shared, it is divided into a series of fix-sized chunks. Each chunk is then
hashed, and the series of hashes allows for accurate verification on the
receiver side.

The size of the chunks is uniform across a single file in the form of 2^N bytes.
