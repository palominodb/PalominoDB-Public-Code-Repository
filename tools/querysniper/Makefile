test_files := $(wildcard t/*.t)

.PHONY: $(test_files)

test: $(test_files)


$(test_files):
	perl -I ../../util/lib -I ../../util/t/lib $@

