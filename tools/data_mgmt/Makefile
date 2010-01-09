TOOL_PATH = ../../util/build

all: pdb-archiver pdb-parted
pdb-archiver:
	perl -I$(TOOL_PATH) $(TOOL_PATH)/unuse_inline_modules < pdb-archiver.in.pl > pdb-archiver.pl
	perl -I$(TOOL_PATH) $(TOOL_PATH)/make_dist pdb-archiver.pl pdb-archiver.pl

pdb-parted:
	perl -I$(TOOL_PATH) $(TOOL_PATH)/unuse_inline_modules < pdb-parted.in.pl > pdb-parted
	perl -I$(TOOL_PATH) $(TOOL_PATH)/make_dist pdb-parted pdb-parted
