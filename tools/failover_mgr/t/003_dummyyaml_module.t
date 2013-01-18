# 003_dummyyaml_module.t
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

use strict;
use warnings FATAL => 'all';
no warnings 'once';
use Test::More tests => 5;
use TestUtil;

BEGIN {
  require_ok($ENV{TOOL});
  fake_use('DSN.pm');
}

my @opts = (
  '-L', 'pdb-test-harness',
  '-m', 'DummyYAML',
  '--dsn', get_files_dir() .'/dsn.yml',
  '--cluster', 'test'
);
eval {
  FailoverManager::main(@opts);
};
is($@, '', 'no croak');
is_deeply(\%DummyYAML::failed_over, {'h=primary_s,u=msandbox,p=msandbox' => 1,'h=failover_s,u=msandbox,p=msandbox' => 1, 'h=slave_s1,u=msandbox,p=msandbox' => 1, 'h=slave_s2,u=msandbox,p=msandbox' => 1}, 'marks slaves as failed over');

%DummyYAML::failed_over = ();
eval {
  unshift @opts, '--pretend';
  FailoverManager::main(@opts);
};
is($@, '', 'no croak');
is_deeply(\%DummyYAML::failed_over, {}, 'marks nothing as failed over');
