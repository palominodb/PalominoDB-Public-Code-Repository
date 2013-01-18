# FailoverPlugin.pm
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
