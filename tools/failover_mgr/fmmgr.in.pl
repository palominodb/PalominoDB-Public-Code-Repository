#!/usr/bin/env perl
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

use strict;
use warnings FATAL => 'all';

# ###########################################################################
# ProcessLog package GIT_VERSION
# ###########################################################################
# ###########################################################################
# End ProcessLog package
# ###########################################################################

# ###########################################################################
# RObj package GIT_VERSION
# ###########################################################################
# ###########################################################################
# End RObj package
# ###########################################################################

# ###########################################################################
# YAMLDSN package GIT_VERSION
# ###########################################################################
# ###########################################################################
# End YAMLDSN package
# ###########################################################################

# ###########################################################################
# DSN package GIT_VERSION
# ###########################################################################
# ###########################################################################
# End DSN package
# ###########################################################################

# ###########################################################################
# Plugin package GIT_VERSION
# ###########################################################################
# ###########################################################################
# End Plugin package
# ###########################################################################

# ###########################################################################
# MysqlSlave package GIT_VERSION
# ###########################################################################
# ###########################################################################
# End MysqlSlave package
# ###########################################################################

# ###########################################################################
# Statistics package GIT_VERSION
# ###########################################################################
# ###########################################################################
# End Statistics package
# ###########################################################################

# ###########################################################################
# FailoverPlugin package GIT_VERSION
# ###########################################################################
# ###########################################################################
# End FailoverPlugin package
# ###########################################################################

# ###########################################################################
# FailoverModule package GIT_VERSION
# ###########################################################################
# ###########################################################################
# End FailoverModule package
# ###########################################################################

# ###########################################################################
# FlipReadOnly package GIT_VERSION
# ###########################################################################
# ###########################################################################
# End FlipReadOnly package
# ###########################################################################

# ###########################################################################
# MoveSlaves package GIT_VERSION
# ###########################################################################
# ###########################################################################
# End MoveSlaves package
# ###########################################################################

# ###########################################################################
# FlipAndMoveSlaves package GIT_VERSION
# ###########################################################################
# ###########################################################################
# End FlipAndMoveSlaves package
# ###########################################################################

# ###########################################################################
# AutoIncrement package GIT_VERSION
# ###########################################################################
# ###########################################################################
# End AutoIncrement package
# ###########################################################################

# ###########################################################################
# ReplicationLag package GIT_VERSION
# ###########################################################################
# ###########################################################################
# End ReplicationLag package
# ###########################################################################

# ###########################################################################
# ReadOnly package GIT_VERSION
# ###########################################################################
# ###########################################################################
# End ReadOnly package
# ###########################################################################

# ###########################################################################
# ProcessCounts package GIT_VERSION
# ###########################################################################
# ###########################################################################
# End ProcessCounts package
# ###########################################################################

# ###########################################################################
# Dummy package GIT_VERSION
# ###########################################################################
# ###########################################################################
# End Dummy package
# ###########################################################################

# ###########################################################################
# DummyYAML package GIT_VERSION
# ###########################################################################
# ###########################################################################
# End DummyYAML package
# ###########################################################################

# ###########################################################################
# DummyComposite package GIT_VERSION
# ###########################################################################
# ###########################################################################
# End DummyComposite package
# ###########################################################################

package FailoverManager;
use strict;
use warnings FATAL => 'all';
our $VERSION = 0.01;

use FailoverPlugin;
use FailoverModule;
use AutoIncrement;
use ProcessCounts;
use ReadOnly;
use ReplicationLag;
use Dummy;
use DummyComposite;
use FlipReadOnly;
use MoveSlaves;
use FlipAndMoveSlaves;


use ProcessLog;
use Plugin;
use DSN;
use YAMLDSN;

use Getopt::Long qw(:config no_ignore_case pass_through no_auto_abbrev);
use Pod::Usage;

sub main {
  @ARGV = @_;
  # pre-loaded plugins
  my @plugins = ( 'AutoIncrement', 'ReplicationLag', 'ReadOnly', 'ProcessCounts' );
  my $pl;

  my $mode;
  my $pretend = 0;
  my $primary_dsn;
  my $failover_dsn;

  my $yaml_dsn = undef;
  my $cluster = undef;

  my $force = 0;

  my $logfile = 'syslog:LOCAL0';

  my $dsnp = DSNParser->default();
  GetOptions(
    'help|h' => sub { pod2usage( -verbose => 99, -noperldoc => 1 ); },
    'mode|m=s' => \$mode,
    'plugin|p=s@' => \@plugins,
    'noplugin|nop=s' => sub {
      my ($on, $v) = @_;
      @plugins = grep { $_ ne $v } @plugins;
    },
    'logfile|L=s' => \$logfile,
    'pretend|n' => \$pretend,
    'primary|pri=s' => \$primary_dsn,
    'failover|fail=s' => \$failover_dsn,
    'dsn|d=s' => \$yaml_dsn,
    'cluster|c=s' => \$cluster,
    'force' => \$force,
  );

  # Create a processlog and make a global ref to it.
  $pl = ProcessLog->new($0, $logfile);
  {
    no strict 'refs';
    no warnings 'once';
    *::PLOG = \$pl;
  }

  # Announce tool name, version, and hash
  $pl->m("fmmgr v$VERSION build: SCRIPT_GIT_VERSION");

  # Check arguments
  unless($mode) {
    pod2usage(-message => '--mode is a mandatory option',
      -verbose => 2, -noperldoc => 1);
    return 1;
  }

  if($yaml_dsn and !$cluster) {
    pod2usage(-message => 'if --dsn is used, then --cluster must be used',
      -verbose => 2, -noperldoc => 1);
    return 1;
  }
  if($yaml_dsn and ($primary_dsn or $failover_dsn)) {
    pod2usage(-message => 'if--dsn is used, you cannot manually specifiy hosts',
      -verbose => 2, -noperldoc => 1);
    return 1;
  }
  if(!$yaml_dsn and !$primary_dsn and !$failover_dsn) {
    pod2usage(-message => '--primary and --failover are required without --dsn',
      -verbose => 2, -noperldoc => 1);
    return 1;
  }

  # Load YAML DSN, if present
  if($yaml_dsn) {
    my $pdb_dsn = YAMLDSN->new($yaml_dsn);
    unless($pdb_dsn->config_username() and $pdb_dsn->config_password()) {
      pod2usage(
        -message => 'YAML DSN needs to have a config section with "username" and "password" keys.',
        -verbose => 2,
        -noperldoc => 1
      );
      return 1;
    }
    $primary_dsn = $dsnp->parse(
      'h=' . $pdb_dsn->cluster_primary($cluster) .
      ',u=' . $pdb_dsn->config_username() .
      ',p=' . $pdb_dsn->config_password()
    );
    $failover_dsn = $dsnp->parse(
      'h=' . $pdb_dsn->cluster_failover($cluster) .
      ',u=' . $pdb_dsn->config_username() .
      ',p=' . $pdb_dsn->config_password()
    );
    foreach my $yml_slave ($pdb_dsn->get_read_hosts($cluster)) {
      push @ARGV, '--slave',
      "h=$yml_slave".
      ',u='. $pdb_dsn->config_username().
      ',p='. $pdb_dsn->config_password();
    }
  }
  # Parse DSN strings from command-line
  else {
    $primary_dsn = $dsnp->parse($primary_dsn);
    $failover_dsn = $dsnp->parse($failover_dsn);
  }


  # Inform plugin and module system of global options
  FailoverPlugin->global_opts($mode, $pretend, $force);
  FailoverModule->global_opts($pretend, $force);

  # Load plugins as necessary
  foreach my $p (@plugins) {
    return 1 if load_plugin($p);
  }


  # Load failover module as necessary
  $pl->d('Trying to load failover module:', $mode);
  if( Plugin::load($mode) ) {
    my $fmopts = {};
    GetOptions($fmopts, $mode->options());
    $mode = $mode->new($primary_dsn, $failover_dsn, $fmopts);
  }
  else {
    $pl->e('Unable to load Failover module:', $mode);
    return 1;
  }

  $pl->m('Using', ref($mode), 'failover module');
  $pl->m('Using plugins:', join(', ', map { ref($_) } @FailoverPlugin::plugins)) if(@FailoverPlugin::plugins);
  
  # Actually run the Failover
  return $mode->run();
}

sub load_plugin {
  my $p = shift;
  $::PLOG->d('Trying to load plugin:', $p);
  if( Plugin::load($p) ) {
    my $popts = {};
    GetOptions($popts, $p->options());
    $p->new($popts);
  }
  else {
    $::PLOG->e('Could not find', $p, 'plugin.');
    return 1;
  }
  return 0;
}

if(!caller) { exit(main(@ARGV)); }

1;

=pod

=head1 NAME

fmmgr - Mysql Failover Manager

=head1 SYNOPSIS

fmmgr --mode <FailoverMethod> [options]

=head1 OPTIONS

=over 8

=item --help,-h

You're looking at it.

=item --mode,-m

The type of failover to perform. This can either be the name of one of the
builtin failover modes, or, a perl module on disk that inherits from the
C<FailoverModule> package. See L<FAILOVER MODULES> for details on that.

The built-in modules are: C<FlipReadOnly>, C<MoveSlaves>,
and C<FlipAndMoveSlaves>.

=item --plugin,-p

Adds a plugin to the end of the list of plugins to run during the failover.
This can either be one of the builtin plugins, or, a perl module on
disk that inherits from the C<FailoverPlugin> package. See
L<PLUGINS> for details.

The built-in plugins are: C<AutoIncrement>, C<ReplicationLag>, C<ReadOnly>,
and C<ProcessCounts> all of which are enabled by default. The built-in
plugins are described in more detail in L<PLUGINS>.

=item --noplugin,-nop

Removes a plugin from the list of plugins to be run. This is almost
the inverse of L<--plugin>, except that it operates anywhere in the
list of plugins.

=item --logfile,-L

Sets the logfile to use. Can be either a path to a file
(existing or otherwise), or a string starting with C<syslog:>,
where the portion after C<syslog:> is the name of the syslog
facility to log to. The facility I<must> be defined beforehand.

Default: syslog:LOCAL0

=item --pretend,-n

Don't perform the failover, just pretend about it.

=item --primary,-pri

Set the primary DSN for this failover. See L<DSNs> below for details
on the format of DSNs.

=item --failover,-fail

Set the failover DSN (secondary master) for this failover. See L<DSNs>
below for details.

=item --dsn,-d

URI to a PDB YAML DSN file. When this option is used L<--cluster> must
also be used. Additionally, L<--primary> and L<--failover> will be
set to the values from the C<primary> and C<failover> keys for the
cluster specified. And, C<< --slave <slave host> >> will be passed
once for each other machine listed as being a readslave for that cluster.

=item --cluster,-c

Specifies the cluster name to use from the YAML DSN file.
See L<--dsn> above.

=item --force

Cause the tool to ignore many types of failures and continue anyway.

=back

=head1 DSNs

=head1 FAILOVER MODULES

Failover modules provide the actual functionality for failing-over
in various ways. The built-in modules, and their options are documented
below. A failover module is specified with L<--mode> and one of the headings.

=head2 FlipReadOnly

This module just simply disables the C<read_only> global variable on the
failover master specified with L<--failover>.

Extra options: I<none>.

Invocation Example:

  fmmgr --mode FlipReadOnly --primary h=dbm,u=failover,p=failover \
                            --failover h=dbs,u=failover,p=failover

This fails over to C<dbs>. Note, even though this module does not require
the use of the primary, you still B<MUST> provide it on the command-line.

=head2 MoveSlaves

This module moves some or all of the read slaves for a cluster off the primary
master and onto the failover master.

Caveat: When using L<--dsn>, B<ALL> slaves will be moved, there is currently
no way to only move some.

Extra options: C<--slave>

The extra option C<--slave> must be specified once for each slave that is
to be moved over to the failover master.

Invocation Example:

  fmmgr --mode MoveSlaves --primary h=dbm,u=failover,p=failover \
                          --failover h=dbs,u=failover,p=failover \
                          --slave h=dbq1,u=failover,p=failover \
                          --slave h=dbq2,u=failover,p=failover

This moves C<dbq1>, and C<dbq2> to slave off of C<dbs>. At this time
all appropriate permissions must already be setup correctly. If they are
not the failover will not be successful and may leave the slaves in an
undefined state. If such a condition occurs, fix the permissions and
re-run this tool with the same parameters.

=head2 FlipAndMoveSlaves

This module composites the actions of the above two modules. It is
functionally equivalent to running the below:

  fmmgr --mode FlipReadOnly --primary h=dbm,u=failover,p=failover \
                            --failover h=dbs,u=failover,p=failover

  fmmgr --mode MoveSlaves --primary h=dbm,u=failover,p=failover \
                          --failover h=dbs,u=failover,p=failover \
                          --slave h=dbq1,u=failover,p=failover \
                          --slave h=dbq2,u=failover,p=failover

However, it does not require running this tool twice.

Extra options: C<--slave>

This is the same as the C<--slave> option for L<MoveSlaves>.

Invocation Example:

  fmmgr --mode FlipAndMoveSlaves --primary h=dbm,u=failover,p=failover \
                          --failover h=dbs,u=failover,p=failover \
                          --slave h=dbq1,u=failover,p=failover \
                          --slave h=dbq2,u=failover,p=failover

This sets C<read_only> to 0 on C<dbs> and causes the slaves C<dbq1>, and
C<dbq2> to slave from C<dbs>.

=head1 PLUGINS

A plugin implements additional business logic I<around> the failover process.
Plugins are not supposed to do any sort of failover on their own. The built-in
plugins are documented below. All of the built-in plugins are enabled by
default. See L<--noplugin> for a way to disable one of them.

=head2 AutoIncrement

This plugin ensures that C<auto_increment_offset> on the primary and failover
database machines is not identical. It aborts the failover if they are unless
L<--force> is used.

=head2 ReplicationLag

This plugin ensures that none of the given database machines (slaves included
for L<MoveSlaves>) are lagging behind their current master.
If lag is found, this plugin aborts the failover. If L<--force> is used,
then this plugin prompts to continue instead.

By default this plugin checks the output of C<SHOW SLAVE STATUS>, however,
as that can be unreliable at times, so, checking a heartbeat table is
also supported. Heartbeat tables are kept up to date by L<http://www.maatkit.org/doc/mk-heartbeat.html> see there for more details.

To use a heartbeat table pass C<--hb_table> and C<--hb_col>.
C<--hb_table> is the C<database.table> of the table to query, and C<--hb_col> is the name of the column containing the datestamp.

=head2 ReadOnly

This plugin ensures that C<read_only> on the failover master is set to 1 before
the failover. It also ensures that C<read_only> is set to 0 after the failover.
This plugin will prompt to continue the failover if C<read_only> is not set to 1
, unless L<--force> is specified.

=head2 ProcessCounts

This plugin is primarily informational in purpose. It provides counts of
processes by user and prompts to continue before running the failover.

=head1 ENVIRONMENT

Setting the environment variable C<Pdb_DEBUG> to a true value will enable
substantially more debugging about the operation of this tool.
When diagnosing problems running with this variable on will help.

=cut
