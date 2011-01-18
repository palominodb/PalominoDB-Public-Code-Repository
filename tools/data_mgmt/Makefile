TOOL_PATH = ../../util/build
LIB_PATH = ../../util/lib
TLIB_PATH = ../../util/t/lib
TOOLS_RELEASE = 0.08
HARNESS_PERL_SWITCHES='-MDevel::Cover=+ignore,.+\.t'

all: bin pdb-archiver pdb-parted pdb-packer pdb-master pdb-zrm-restore pdb-sandbox-merge pdb-munch

test:
	prove -I$(LIB_PATH) -It/lib -Isrc -I$(TLIB_PATH) t/*
	cover

test-no-cover:
	prove -I$(LIB_PATH) -It/lib -Isrc -I$(TLIB_PATH) t/*

bin:
	mkdir bin

pdb-archiver:
	perl -I$(TOOL_PATH) $(TOOL_PATH)/make_dist src/pdb-archiver.in.pl bin/pdb-archiver

pdb-parted:
	perl -I$(TOOL_PATH) $(TOOL_PATH)/make_dist src/pdb-parted.in.pl bin/pdb-parted

pdb-packer:
	perl -I$(TOOL_PATH) $(TOOL_PATH)/make_dist src/pdb-packer.in.pl bin/pdb-packer

pdb-master:
	perl -I$(TOOL_PATH) $(TOOL_PATH)/make_dist src/pdb-master.in.pl bin/pdb-master

pdb-zrm-restore:
	perl -I$(TOOL_PATH) $(TOOL_PATH)/make_dist src/pdb-zrm-restore.in.pl bin/pdb-zrm-restore

pdb-sandbox-merge:
	perl -I$(TOOL_PATH) $(TOOL_PATH)/make_dist src/pdb-sandbox-merge.in.pl bin/pdb-sandbox-merge

pdb-munch:
	perl -I$(TOOL_PATH) $(TOOL_PATH)/make_dist src/pdb-munch.in.pl bin/pdb-munch

pdb-map-tables:
	perl -I$(TOOL_PATH) $(TOOL_PATH)/make_dist src/pdb-map-tables.in.pl bin/pdb-map-tables

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
