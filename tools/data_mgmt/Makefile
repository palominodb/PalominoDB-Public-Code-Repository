TOOL_PATH = ../../util/build
LIB_PATH = ../../util/lib

all: pdb-archiver pdb-parted
pdb-archiver:
	perl -I$(TOOL_PATH) $(TOOL_PATH)/unuse_inline_modules < src/pdb-archiver.in.pl > bin/pdb-archiver
	perl -I$(TOOL_PATH) $(TOOL_PATH)/make_dist $(LIB_PATH) bin/pdb-archiver bin/pdb-archiver

pdb-parted:
	perl -I$(TOOL_PATH) $(TOOL_PATH)/unuse_inline_modules < src/pdb-parted.in.pl > bin/pdb-parted
	perl -I$(TOOL_PATH) $(TOOL_PATH)/make_dist $(LIB_PATH) bin/pdb-parted bin/pdb-parted

rpmball:
	pushd ../../
	tar -czvf tools/pdb-data-management.tar.gz tools/data_mgmt/src tools/data_mgmt/t util/lib util/t
	mv tools/pdb-data-management.tar.gz tools/data_mgmt/
	popd
