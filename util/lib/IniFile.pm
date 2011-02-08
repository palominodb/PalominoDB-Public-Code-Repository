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
package IniFile;
use strict;
use warnings FATAL => 'all';
use File::Glob;

=pod

=head1 NAME

IniFile - Reads an enhanced mysql style .cnf file.

=head1 SYNOPSIS

The IniFile module reads a mysql .cnf style config file,
with a few enhancements to make it more general purpose.

Example config (at /etc/myapp.local.cnf):

  [global]
    path = /usr/lib
    no-startup-files
    load[1] = module_a.so
    load[2] = module_b.so

  [regional]
    skip-bad-setting

  !include /etc/myapp.defaults.cnf
  !includedir /etc/myapp.d

Code to load it:

  use IniFile;
  use Data::Dumper;

  my %c = IniFile::read_config('/etc/myapp.local.cnf');
  print Dumper(\%c);
  #Output:
  #$VAR1 = {
  #  'global' => {
  #    'startup-files' => 0,
  #    'path' => '/usr/lib',
  #    'load' => [
  #      'module_a.so',
  #      'module_b.so'
  #    ],
  #    'setting-from-myapp_d' => 1
  #  },
  #  'regional' => {
  #    'bad-setting' => 0,
  #    'setting-from-defaults' => 'bill'
  #  }
  #};

A few things worth noting here:

=over 4

=item Sections are supported like you'd expect.

Anything not in a section goes into an empty section (named C<''>).
This is only unintuitive part of sections.

=item MySQL style no- and skip- prefixes do what you expect.

Namely, you get the option name C<bad-setting>, with a value of C<0>.

=item !include and !includedir load single and multiple files, respectively.

=item The same key can be specified multiple times with an 'index' (load).

At the time of this writing the value and order of the index is unimportant,
the order in which the keys are listed is. So, the above example could have
easily been:

  load[10]=module1.so
  load[8] =module2.so

And the resultant list would have been the same.

=back

=cut

# Loads a my.cnf into a hash.
# of the form:
# key: group
# val: { <option> => <value> }
# Strips spaces and newlines.
sub read_config {
  my $file = shift;
  my %cfg;
  my $inif;
  unless(open $inif, "<$file") {
    return undef;
  }
  my $cur_sec = '';
  while(<$inif>) {
    chomp;
    next if(/^\s*(?:;|#)/);
    next if(/^$/);
    if(/^\s*\[(\w+)\]/) { # Group statement
      $cfg{$1} = {};
      $cur_sec = $1;
    }
    elsif(/^!(include(?:dir)?)\s+([^\0]+)/) { # include directives
      my $path = $2;
      my @files;
      if($1 eq 'includedir') {
        @files = glob($path . "/*.cnf");
      }
      else {
        @files = ($path);
      }
      for(@files) { _merge(\%cfg, {read_config($_)}); }
    }
    else { # options and flags
      my ($k, $v) = split(/=/, $_, 2);
      $k =~ s/\s+$//;
      $k =~ s/^\s+//;
      if(defined($v)) {
        $v =~ s/^\s+//;
        $v =~ s/\s?#.*?[^"']$//;
        $v =~ s/^(?:"|')//;
        $v =~ s/(?:"|')$//;
      }
      else {
        if($k =~ /^(?:no-|skip-)(.*)/) {
          $k = $1;
          $v = 0;
        }
        else {
          $v = 1;
        }
      }
      # sanity check because newlines in these would be weird.
      chomp($k); chomp($v);

      # numbered option keys create a list in the result.
      if($k =~ /^(.*?)\s*\[\s*\d+\s*\]/) {
        $k = $1;
        push @{$cfg{$cur_sec}{$k}}, $v;
        next;
      }
      $cfg{$cur_sec}{$k} = $v;
    }
  }
  return %cfg;
}

# Dumb in-place merge of %$h2 into %$h1
# Returns $h1
sub _merge {
  my ($h1, $h2, $p) = @_;
  foreach my $k (keys %$h2) {
    # New section
    if(not $p and not exists $h1->{$k}) {
      $h1->{$k} = $h2->{$k};
    }
    # Existing section needs merge
    elsif(not $p and exists $h1->{$k}) {
      _merge($h1->{$k}, $h2->{$k}, $h1);
    }
    # Override existing keys
    elsif($p) {
      $h1->{$k} = $h2->{$k};
    }
  }
  $h1;
}

1;
