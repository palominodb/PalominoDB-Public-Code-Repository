# FlipAndMoveSlaves.pm
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
