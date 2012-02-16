TOOL_PATH = "../../util/build"
all: pdb-dsn-checksum

pdb-dsn-checksum: pdb-dsn-checksum.in.pl dist
	perl -I $(TOOL_PATH) $(TOOL_PATH)/insert_module ../../util/lib ProcessLog < pdb-dsn-checksum.in.pl > tmp_dist
	perl -I $(TOOL_PATH) $(TOOL_PATH)/insert_module ../../dsn/perl/lib Pdb::DSN < tmp_dist > dist/pdb-dsn-checksum
	perl -I $(TOOL_PATH) $(TOOL_PATH)/unuse_inline_modules dist/pdb-dsn-checksum dist/pdb-dsn-checksum
	chmod +x dist/pdb-dsn-checksum
	rm -f tmp_dist

dist:
	@mkdir dist
