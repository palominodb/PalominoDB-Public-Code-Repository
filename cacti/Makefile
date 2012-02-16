# Add new scripts to be patched to this list here.
# Then add a rule at the bottom for building a target of the same name.
SCRIPTS = ss_get_mysql_stats.php

###############################################################################
## NO NEED TO EDIT BETWEEN HERE
###############################################################################
# Modify DISTDIR to change where patched scripts are placed.
# Modify SCRIPTDIR to control where scripts are located.
# Trailing slashes are important.
DISTDIR     = dist/
SCRIPTDIR   = scripts/
DISTSCRIPTS = $(addprefix $(DISTDIR), $(SCRIPTS))
SCRIPTSPATH= $(addprefix $(SCRIPTDIR), $(SCRIPTS))
.PHONY: check-scripts $(SCRIPTS)

# Automation for copying patched files to dist/
all: check-scripts dist $(DISTSCRIPTS)
$(DISTSCRIPTS): $(SCRIPTS)
	cp $(SCRIPTSPATH) $(DISTDIR)/
	git checkout -- $(SCRIPTSPATH)

check-scripts:
	@echo "[check-files]: If make dies in here, it means you haven't committed all your changes!"
	git diff-files --quiet $(SCRIPTSPATH)

dist:
	mkdir -p $(DISTDIR)

clean:
	rm -rf $(DISTDIR)
###############################################################################
## AND HERE, UNLESS YOU'RE FIXING A BUG, OR ADDING FUNCTIONALITY.
###############################################################################

# Rules for patching individual scripts follow
ss_get_mysql_stats.php:
	patch -p2 < patches/ss_get_mysql_stats_mkheartbeat_neg.patch
