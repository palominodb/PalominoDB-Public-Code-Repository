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
package FailoverPlugin;
use strict;
use warnings FATAL => 'all';
use ProcessLog;
use DBI;

# Global list 
our @plugins;
our ($mode, $pretend, $force);

# localize this variable in a block
# to temporarily disable hooks.
our $no_hooks = 0;

# Create new plugin with options
# parsed from options() sub.
# plugins are allowed to die here to signal
# that they needed something.
sub new {
  my $class = shift;
  my $opts = shift || {};
  push @plugins, bless($opts, $class);
  $::PLOG->d('Instantiating:', $plugins[-1]);
  return $plugins[-1];
}

sub DESTROY {
  my $self = shift;
  $::PLOG->d('Destroying:', $self);
}

sub global_opts {
  my $class = shift;
  ($mode, $pretend, $force) = @_;
}

# Overriden in plugins to return options
# the plugin requires.
sub options {
  return ();
}

# #########################################################
# Plugin hooks
# ------------
# Each hook is called with the primary and failover DBH
# connections and then any other DBH connections made
# by a failover module.
# #########################################################

# Implemented in plugins to do business logic.
sub pre_verification {
  return undef;
}

# Implemented in plugins to do business logic.
sub post_verification {
  return undef;
}

# Implemented in plugins to do business logic.
sub begin_failover {
  return undef;
}

# Implemented in plugins to do business logic.
sub finish_failover {
  return undef;
}

# Called by failover modules to run plugin
# hooks at the pre_verification phase.
sub pre_verification_hook {
  my $class = shift;
  return 1 if $no_hooks;
  foreach my $p (@plugins) {
    $::PLOG->d('Running pre-verification hook for ', $p);
    $p->pre_verification(@_);
  }
}

# Called by failover modules to run plugin
# hooks at the post_verification phase.
sub post_verification_hook {
  my $class = shift;
  return 1 if $no_hooks;
  foreach my $p (@plugins) {
    $::PLOG->d('Running post-verification hook for ', $p);
    $p->post_verification(@_);
  }
}

# Called by failover modules to run plugin
# hooks at the begin_failover phase.
sub begin_failover_hook {
  my $class = shift;
  return 1 if $no_hooks;
  $::PLOG->i('Beginning failover for', scalar caller);
  foreach my $p (@plugins) {
    $::PLOG->d('Running begin-failover hook for ', $p);
    $p->begin_failover(@_);
  }
}

# Called by failover modules to run plugin
# hooks at the finish_failover phase.
sub finish_failover_hook {
  my $class = shift;
  return 1 if $no_hooks;
  $::PLOG->i('Finishing failover for', scalar caller);
  foreach my $p (@plugins) {
    $::PLOG->d('Running finish-failover hook for ', $p);
    $p->finish_failover(@_);
  }
}

1;
