TOOL_PATH = ../../util/build
LIB_PATH = ../../util/lib
TLIB_PATH = ../../util/t/lib
TOOLS_RELEASE = 0.07
HARNESS_PERL_SWITCHES='-MDevel::Cover=+ignore,.+\.t'

all: dirs pdb-archiver pdb-parted pdb-packer pdb-master pdb-zrm-restore pdb-sandbox-merge pdb-munch

test:
	prove -I$(LIB_PATH) -It/lib -Isrc -I$(TLIB_PATH) t/*
	cover

dirs:
	-mkdir bin

pdb-archiver:
	perl -I$(TOOL_PATH) $(TOOL_PATH)/make_dist $(LIB_PATH) src/pdb-archiver.in.pl bin/pdb-archiver
	perl -I$(TOOL_PATH) $(TOOL_PATH)/unuse_inline_modules bin/pdb-archiver bin/pdb-archiver

pdb-parted:
	perl -I$(TOOL_PATH) $(TOOL_PATH)/make_dist $(LIB_PATH) src/pdb-parted.in.pl bin/pdb-parted
	perl -I$(TOOL_PATH) $(TOOL_PATH)/unuse_inline_modules bin/pdb-parted bin/pdb-parted

pdb-packer:
	perl -I$(TOOL_PATH) $(TOOL_PATH)/make_dist $(LIB_PATH) src/pdb-packer.in.pl bin/pdb-packer
	perl -I$(TOOL_PATH) $(TOOL_PATH)/unuse_inline_modules bin/pdb-packer bin/pdb-packer

pdb-master:
	perl -I$(TOOL_PATH) $(TOOL_PATH)/make_dist $(LIB_PATH) src/pdb-master.in.pl bin/pdb-master
	perl -I$(TOOL_PATH) $(TOOL_PATH)/unuse_inline_modules bin/pdb-master bin/pdb-master

pdb-zrm-restore:
	perl -I$(TOOL_PATH) $(TOOL_PATH)/make_dist $(LIB_PATH) src/pdb-zrm-restore.in.pl bin/pdb-zrm-restore
	perl -I$(TOOL_PATH) $(TOOL_PATH)/unuse_inline_modules bin/pdb-zrm-restore bin/pdb-zrm-restore

pdb-sandbox-merge:
	perl -I$(TOOL_PATH) $(TOOL_PATH)/make_dist $(LIB_PATH) src/pdb-sandbox-merge.in.pl bin/pdb-sandbox-merge
	perl -I$(TOOL_PATH) $(TOOL_PATH)/unuse_inline_modules bin/pdb-sandbox-merge bin/pdb-sandbox-merge

pdb-munch:
	perl -I$(TOOL_PATH) $(TOOL_PATH)/make_dist $(LIB_PATH) src/pdb-munch.in.pl bin/pdb-munch
	perl -I$(TOOL_PATH) $(TOOL_PATH)/unuse_inline_modules bin/pdb-munch bin/pdb-munch

rpmball:
	mkdir -pv pdb-data-management-$(TOOLS_RELEASE)
	mkdir pdb-data-management-$(TOOLS_RELEASE)/util/
	mkdir -p pdb-data-management-$(TOOLS_RELEASE)/tools/data_mgmt
	cp -r Makefile pdb-data-management-$(TOOLS_RELEASE)/tools/data_mgmt/
	cp -r src pdb-data-management-$(TOOLS_RELEASE)/tools/data_mgmt/
	cp -r t pdb-data-management-$(TOOLS_RELEASE)/tools/data_mgmt/
	cp -r ../../util/lib pdb-data-management-$(TOOLS_RELEASE)/util/
	cp -r ../../util/t pdb-data-management-$(TOOLS_RELEASE)/util/
	cp -r ../../util/build pdb-data-management-$(TOOLS_RELEASE)/util/
	cp -r ../../util/gitver.sh pdb-data-management-$(TOOLS_RELEASE)/util/
	cp pdb-data-management.spec pdb-data-management-$(TOOLS_RELEASE)
	tar czvf pdb-data-management-$(TOOLS_RELEASE).tar.gz pdb-data-management-$(TOOLS_RELEASE)/
	rm -rf pdb-data-management-$(TOOLS_RELEASE)/
