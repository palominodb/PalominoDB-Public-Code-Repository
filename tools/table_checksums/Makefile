TOOL_PATH = $(PDB_CODE_ROOT)/util/build
all: pdb-dsn-checksum

pdb-dsn-checksum: pdb-dsn-checksum.in.pl
	$(TOOL_PATH)/make_dist pdb-dsn-checksum.in.pl pdb-dsn-checksum
	chmod +x pdb-dsn-checksum
