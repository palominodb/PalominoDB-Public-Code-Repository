#!/bin/bash
#
# Makes a tarball suitable for passing to 'rpmbuild -tb <tarball>'
# And hopefully good enough to make a debian package out of, too.
#

version=$(cat zrm-innobackupex.spec | grep -E '^Version' | awk '{ print $2 }')

rm -rf zrm-innobackupex-$version
rm -rf zrm-innobackupex-$version.tar.gz

mkdir zrm-innobackupex-$version

rsync -aP debian examples plugins Makefile CHANGELOG README zrm-innobackupex.spec zrm-innobackupex-$version/

tar czvf zrm-innobackupex-$version.tar.gz zrm-innobackupex-$version/