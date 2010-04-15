TOOL_PATH = ../../util/build
UTILTEST_PATH = ../../util/t
UTILLIB_PATH  = ../../util/lib
DSNLIB_PATH = ../../dsn/perl/lib

DEPS = $(UTILLIB_PATH)/ProcessLog.pm \
			 $(UTILLIB_PATH)/DSN.pm \
			 $(UTILLIB_PATH)/Plugin.pm \
			 $(UTILLIB_PATH)/MysqlSlave.pm \
			 $(UTILLIB_PATH)/Statistics.pm \
			 $(DSNLIB_PATH)/Pdb/DSN.pm \
			 lib/FailoverPlugin.pm \
			 lib/FailoverModule.pm \
			 modules/FlipReadOnly.pm \
			 modules/MoveSlaves.pm \
			 modules/FlipAndMoveSlaves.pm \
			 modules/Dummy.pm \
			 modules/DummyYAML.pm \
			 modules/DummyComposite.pm \
			 plugins/AutoIncrement.pm \
			 plugins/ProcessCounts.pm \
			 plugins/ReadOnly.pm \
			 plugins/ReplicationLag.pm

export HARNESS_PERL_SWITCHES='-MDevel::Cover=+ignore,.+\.t'

all: fmmgr

clean:
	rm -rf fmmgr

fmmgr: fmmgr.in.pl $(DEPS)
	perl -I$(TOOL_PATH) $(TOOL_PATH)/make_dist -L lib -L plugins -L modules -L $(UTILLIB_PATH) -L $(DSNLIB_PATH) fmmgr.in.pl fmmgr
	perl -I$(TOOL_PATH) $(TOOL_PATH)/unuse_inline_modules fmmgr fmmgr
	chmod +x fmmgr

test: testsrc testbin

testsrc:
	TOOL=fmmgr.in.pl prove -I lib -I plugins -I modules -I$(UTILLIB_PATH) -I$(UTILTEST_PATH)/lib -I $(DSNLIB_PATH) t/*
	cover

testbin: fmmgr
	TOOL=./fmmgr prove -I lib -I plugins -I modules -I$(UTILLIB_PATH) -I$(UTILTEST_PATH)/lib -I $(DSNLIB_PATH) t/*
	cover
