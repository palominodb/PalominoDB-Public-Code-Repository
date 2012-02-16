TOOL_PATH = ../../util/build
LIB_PATH  = ../../util/lib

all:
	perl -I $(TOOL_PATH) $(TOOL_PATH)/make_dist $(LIB_PATH) table_partitions.pl table_partitions
	perl $(TOOL_PATH)/unuse_inline_modules table_partitions table_partitions
