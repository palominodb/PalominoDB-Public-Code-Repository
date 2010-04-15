#!/usr/bin/env perl
use strict;
use warnings FATAL => 'all';

# ###########################################################################
# ProcessLog package GIT_VERSION
# ###########################################################################
# ###########################################################################
# End ProcessLog package
# ###########################################################################

# ###########################################################################
# Pdb::DSN package GIT_VERSION
# ###########################################################################
# ###########################################################################
# End Pdb::DSN package
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
use Pdb::DSN;

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
  no strict 'refs';
  no warnings 'once';
  *::PLOG = \$pl;
  use warnings FATAL => 'all';
  use strict;

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
    my $pdb_dsn = Pdb::DSN->new($yaml_dsn);
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
    $pl->d('Trying to load plugin:', $p);
    if( Plugin::load($p) ) {
      my $popts = {};
      GetOptions($popts, $p->options());
      $p->new($popts);
    }
    else {
      $pl->e('Could not find', $p, 'plugin.');
      return 1;
    }
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

=head1 PLUGINS

=head1 ENVIRONMENT

=cut
