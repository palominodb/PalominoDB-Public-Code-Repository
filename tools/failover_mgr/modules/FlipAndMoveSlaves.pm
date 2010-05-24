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
package FlipAndMoveSlaves;
use strict;
use warnings FATAL => 'all';
use ProcessLog;
use Carp;
use DSN;
use FailoverPlugin;
use FailoverModule;
use FlipReadOnly;
use MoveSlaves;
our @ISA = qw(FailoverModule);

our $pretend;

sub new {
  my ($class, $pri_dsn, $fail_dsn, $opts) = @_;
  my $self = bless $class->SUPER::new($pri_dsn, $fail_dsn, $opts), $class;
  croak('Required flag --slave missing') unless $opts->{'slave'};
  my $dsnp = DSNParser->default();
  @{$self->{'slave'}} = map { if(ref($_) and ref($_) eq 'DSN') { $_; } else { $_ = $dsnp->parse($_); $_->fill_in($pri_dsn); } } @{$self->{'slave'}};
  return $self;
}

sub options {
  return ( 'slave|s=s@' );
}

sub run {
  my ($self) = @_;

  my $pdsn = $self->{'primary_dsn'};
  my $fdsn = $self->{'failover_dsn'};
  my @slaves = @{$self->{'slave'}};

  FailoverPlugin->pre_verification_hook($pdsn, $fdsn, @{$self->{'slave'}});
  FailoverPlugin->begin_failover_hook($pdsn, $fdsn, @{$self->{'slave'}});

  my $flipRO = FlipReadOnly->new($pdsn, $fdsn);
  my $moveSlaves = MoveSlaves->new($pdsn, $fdsn, { 'slave' => $self->{'slave'} });
  eval {
    {
      # Make sure other failover modules don't run hooks.
      local $FailoverPlugin::no_hooks = 1;
      $flipRO->run();
      $moveSlaves->run();
    };
  };

  if($@) {
    $::PLOG->e(__PACKAGE__, 'failover FAILED');
    $::PLOG->e('Got error:', $@);
    FailoverPlugin->finish_failover_hook(0, $pdsn, $fdsn, @{$self->{'slave'}});
    FailoverPlugin->post_verification_hook(0, $pdsn, $fdsn, @{$self->{'slave'}});
    croak($@);
  }
  FailoverPlugin->finish_failover_hook(1, $pdsn, $fdsn, @{$self->{'slave'}});
  FailoverPlugin->post_verification_hook(1, $pdsn, $fdsn, @{$self->{'slave'}});

  return 0;
}

1;
