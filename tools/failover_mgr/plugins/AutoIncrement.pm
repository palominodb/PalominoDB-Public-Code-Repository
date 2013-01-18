# AutoIncrement.pm - crepsucule
# Copyright (C) 2009-2013 PalominoDB, Inc.
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
 
package AutoIncrement;
use strict;
use warnings FATAL => 'all';
use ProcessLog;
use Exporter;
use MysqlSlave;
use Carp;
use FailoverPlugin;
our @ISA = qw(FailoverPlugin);

sub pre_verification {
  my ($self, $pri_dsn, $fail_dsn) = @_;

  my $pri_s = MysqlSlave->new($pri_dsn);
  my $fail_s = MysqlSlave->new($fail_dsn);
  if($pri_s->auto_inc_off() == $fail_s->auto_inc_off()) {
    $::PLOG->e($pri_dsn->get('h'), 'auto_increment_offset:', $pri_s->auto_inc_off());
    $::PLOG->e($fail_dsn->get('h'), 'auto_increment_offset:', $fail_s->auto_inc_off());
    if($FailoverPlugin::force) {
      $::PLOG->i('Continuing due to --force being passed.');
    }
    croak('Failed pre-verification check: auto_increment_offset') unless($FailoverPlugin::force);
  }
  else {
    $::PLOG->m($pri_dsn->get('h'), 'auto_increment_offset:', $pri_s->auto_inc_off());
    $::PLOG->m($fail_dsn->get('h'), 'auto_increment_offset:', $fail_s->auto_inc_off());
  }
}

1;
