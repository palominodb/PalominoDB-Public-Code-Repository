# NagiosAlert.pm
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

package NagiosAlert;
# Highly simplistic perl module to send passive nagios alerts.
# Depends on the C tool 'send_nsca', though, it could be made to not do that.
# Ideally, existing work could be leveraged, but, installing perl modules is a pain.

use strict;
use warnings;

use lib qw(../../tools/data_mgmt);
use ProcessLog;

use constant OK       => 0;
use constant WARNING  => 1;
use constant CRITICAL => 2;
use constant UNKNOWN  => 3;

sub new {
  my ($class, $plog, $args) = @_;
  $args ||= {};
  $args->{send_nsca} ||= qx/which send_nsca/;
  $args->{nsca_cfg} ||= qq(/etc/nagios/nsca.cfg);
  $args->{nagios_host} ||= qq(nagios);
  $args->{plog} = $plog;

  bless $args, $class;
  return $args;
}

sub send {
  my ($self, $host, $service, $status, @rest) = @_;

  @rest = $self->_quote(@rest);
  my $res = { code => -1, output => '', evalerr => '' };
  my $cmd = qq#echo -e '$host\\t$service\\t$status\\t@rest' |#;
  $cmd   .= qq#$self->{send_nsca} -H $self->{nagios_host} -c $self->{nsca_cfg}#;
  $self->{plog}->d("Execing: ", $cmd);
  $res = $self->{plog}->xs($cmd);
  if(($res->{code} >> 8) != 0) {
    $self->{plog}->es("Error ($res->{code}) sending nagios alert.", "Output:",$res->{output});
  }
  return $res;
}

# Escape the naughty characters.
sub _quote {
  my ($self, @naughty) = @_;
  map {
    s/'/\\'/g;
    s/\n/\\n/g;
  } @naughty;
  return @naughty;
}

1;
