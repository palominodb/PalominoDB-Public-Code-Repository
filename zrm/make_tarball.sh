#!/bin/bash
# make_tarball.sh - Makes a tarball suitable for passing to 'rpmbuild -tb
# <tarball>' And hopefully good enough to make a debian package out of, too.
#
# Copyright (C) 2013 PalominoDB, Inc.
#
# You may contact the maintainers at eng@palominodb.com.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

version=$(cat zrm-innobackupex.spec | grep -E '^Version' | awk '{ print $2 }')

rm -rf zrm-innobackupex-$version
rm -rf zrm-innobackupex-$version.tar.gz

mkdir zrm-innobackupex-$version

rsync -aP debian examples plugins Makefile CHANGELOG README zrm-innobackupex.spec zrm-innobackupex-$version/

tar czvf zrm-innobackupex-$version.tar.gz zrm-innobackupex-$version/
