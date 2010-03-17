# Copyright (c) 2009-2010, PalominoDB, Inc.
# All rights reserved.
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
# 
#   * Redistributions of source code must retain the above copyright notice,
#     this list of conditions and the following disclaimer.
# 
#   * Redistributions in binary form must reproduce the above copyright notice,
#     this list of conditions and the following disclaimer in the documentation
#     and/or other materials provided with the distribution.
# 
#   * Neither the name of PalominoDB, Inc. nor the names of its contributors
#     may be used to endorse or promote products derived from this software
#     without specific prior written permission.
# 
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
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
