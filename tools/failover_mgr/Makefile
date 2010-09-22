TOOL_PATH = ../../util/build
UTILTEST_PATH = ../../util/t
UTILLIB_PATH  = ../../util/lib
PERL = perl

DEPS = $(UTILLIB_PATH)/ProcessLog.pm \
			 $(UTILLIB_PATH)/DSN.pm \
			 $(UTILLIB_PATH)/Plugin.pm \
			 $(UTILLIB_PATH)/MysqlSlave.pm \
			 $(UTILLIB_PATH)/Statistics.pm \
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
	$(PERL) -I$(TOOL_PATH) $(TOOL_PATH)/make_dist -L lib -L plugins -L modules -L $(UTILLIB_PATH) fmmgr.in.pl fmmgr
	$(PERL) -I$(TOOL_PATH) $(TOOL_PATH)/unuse_inline_modules fmmgr fmmgr
	chmod +x fmmgr

test: testsrc testbin

testsrc:
	TOOL=fmmgr.in.pl prove -I lib -I plugins -I modules -I$(UTILLIB_PATH) -I$(UTILTEST_PATH)/lib t/*
	cover

testbin: fmmgr
	TOOL=./fmmgr prove -I lib -I plugins -I modules -I$(UTILLIB_PATH) -I$(UTILTEST_PATH)/lib t/*
	cover
