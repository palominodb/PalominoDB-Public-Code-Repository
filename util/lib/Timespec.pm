# Copyright (c) 2011, PalominoDB, Inc.
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
package Timespec;
use strict;
use warnings FATAL => 'all';
use DateTime;
use DateTime::Format::Strptime;

sub parse {
  my ($class, $str, $ref) = @_;
  # ref is the reference time by which to compare
  # relative timespecs. Normally it can be left blank,
  # but, when testing we need to validate against pre-computed
  # ranges.
  $ref ||= DateTime->now(time_zone => 'local');
  my $fmt = DateTime::Format::Strptime->new(pattern => '%F %T', time_zone => 'local');
  if($str =~ /^([-+]?)(\d+)([hdwmqy])(?:\s(startof))?$/) {
    my ($spec, $amt) = ($3, $2);
    my %cv = ( 'h' => 'hours', 'd' => 'days', 'w' => 'weeks', 'm' => 'months', 'y' => 'years' );
    if($4) {
      if($cv{$spec}) {
        $ref->truncate(to => $cv{$spec});
      }
      else { # quarters
        $ref->truncate(to => 'day');
        $ref->subtract(days => $ref->day_of_quarter()-1);
      }
    }

    # for some reason, quarters are not first-class citizens in DateTime
    # so, we have to work around it by doing some math ourselves.
    if($spec eq 'q') {
      $spec = 'm';
      $amt *= 3;
    }

    if($1 eq '-') {
      $ref->subtract($cv{$spec} => $amt);
    }
    if($1 eq '+' or $1 eq '') {
      $ref->add($cv{$spec} => $amt);
    }
    return $ref;
  }
  elsif($_ = $fmt->parse_datetime($str)) {
    return $_;
  }
}

1;
