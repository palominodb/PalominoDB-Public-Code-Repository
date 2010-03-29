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
# IniFile package GIT_VERSION
# ###########################################################################
# ###########################################################################
# End IniFile package
# ###########################################################################

# ###########################################################################
# RObj package GIT_VERSION
# ###########################################################################
# ###########################################################################
# End RObj package
# ###########################################################################

# ###########################################################################
# MysqlInstance package GIT_VERSION
# ###########################################################################
# ###########################################################################
# End MysqlInstance package
# ###########################################################################

# ###########################################################################
# MysqlMasterInfo package GIT_VERSION
# ###########################################################################
# ###########################################################################
# End MysqlMasterInfo package
# ###########################################################################

#package Worker;
#
#sub new {
#  my ($class, $pl, $dry_run, $host, $master_host,
#    $sandbox, $repl_user, $repl_host) = @_;
#}
#
#1;
#
package pdb_master;
use strict;
use warnings FATAL => 'all';

use vars qw($VERSION);
$VERSION = 0.01;

use Getopt::Long qw(GetOptionsFromArray :config no_ignore_case);
use Pod::Usage;
use POSIX ':sys_wait_h';
use Data::Dumper;

use ProcessLog;
use IniFile;
use MysqlInstance;
use MysqlMasterInfo;
use RObj;

my $ssh_key = undef;
my @save_dbs = qw(mysql);
my $save_dest = '/tmp';
my $save_user = 'root';
my $save_pass = undef;

# User/Pw for all hosts in cluster
# to use for replication
my ($repl_user, $repl_pass);

sub main {
  my @ARGV = @_;
  my $sandbox_path = undef;
  my (@hosts, @masters, @pids);
  my $dry_run = 0;
  my $pl;
  GetOptionsFromArray(\@ARGV,
    'help|h|?'  => sub { pod2usage( -verbose => 1); },
    'dry-run|n' => \$dry_run,
    'ssh-key|i=s' => \$ssh_key,
    'save|S=s@' => \@save_dbs,
    'save-destination|D=s' => \$save_dest,
    'save-user=s' => \$save_user,
    'save-password=s' => \$save_pass,
    'repl-user=s' => \$repl_user,
    'repl-password=s' => \$repl_pass
  );
  if(scalar @ARGV < 3) {
    pod2usage(-message => "Must have a sandbox and at least two host references.",
      -verbose => 1);
  }
  $sandbox_path = shift @ARGV;
  @hosts        = @ARGV;
  if(! -d $sandbox_path or ! -f "$sandbox_path/my.sandbox.cnf" ) {
    pod2usage(-message => "First argument must be a sandbox directory.",
      -verbose => 1);
  }

  $pl = ProcessLog->new($0, '/dev/null', undef);
  @hosts = map { MysqlInstance->new(parse_hostref($_), $ssh_key) } @hosts;
  @masters = @hosts[0,1];
  $pl->i("pdb-master v$VERSION build SCRIPT_GIT_VERSION");
  foreach my $host (@hosts) {
    #my %remote_cfg
    #my $rmi = RObj->new($host->{host}, $host->{user}, $host->{ssh_key});
    #$rmi->add_package('MysqlMasterInfo');
    #$rmi->add_main(sub { MysqlMasterInfo->open(@_); });
    #$master_info = $rmi->do($remote_cfg{'mysqld'}{'master-info-file'}
    #  || $remote_cfg{'mysqld'}{'datadir'} . '/master.info');
    my $pid = fork();
    if($pid == 0) {
      return worker($pl, $dry_run, $host, $sandbox_path);
    }
    $pl->d('Spawned worker:', $pid);
    die('Unable to spawn worker process.') unless($pid);
    push @pids, $pid;
  }
  my $kid;
  while( ($kid = waitpid(-1, 0)) >= 0 ) {
    # If there was an error with any of the workers,
    # kill them all!
    $pl->d('Return code:', ($? >> 8));
    if( ($? >> 8) > 0 ) {
      for(@pids) {
        kill(15, $_); # Send SIGTERM
      }
      $pl->e('One of the workers encountered an error.');
    }
    else {
      $pl->d('Worker:', $kid, 'finished.');
    }
  }
  $pl->i('pdb-master finished');
  return 0;
}

if(!caller) { main(@ARGV); }

sub exit_worker {
  my ($host, $pl, $code, $step) = @_;
  # Descriptions
  my @steps = ('not started', 'mysql stopped', 'got config and master.info',
    'required databases saved', 'data removed', 'data copied',
    'removed old ib_logfiles', 'restarted mysql');
  $pl->i("$host->{host}:", 'worker exited at step:', $steps[$step], '(', $step, ')');
  exit($code);
}

sub worker {
  my ($pl, $dry_run, $host, $sandbox_path) = @_;
  my ($master_info, %remote_cfg, $rmi, $res);
  my $saved_dbs_file;
  # exit flag - if true exit at the next
  # safe spot.
  my $exit = 0;
  # Step is which step of the rebuild
  # This process is at.
  my $step = 0;

  my $sig_handler = sub {
    $pl->m("$host->{host}:", 'Caught Signal - cleaning up.');
    $exit = 1;
  };
  $pl->d("Installing signal handlers..");
  local $SIG{TERM} = $sig_handler;
  local $SIG{HUP}  = $sig_handler;
  local $SIG{QUIT} = $sig_handler;
  local $SIG{INT}  = $sig_handler;

  my $status = $host->status;
  $pl->i("$host->{host} status: ", $status ? 'Not Running' : 'Running');
  unless($dry_run) {
    if($host->stop and $status == 0) {
      $pl->e("Unable to stop mysql on $host->{host}. Exiting worker.");
      return 1;
    }
  }
  $step++;
  exit_worker($host, $pl, 1, $step) if($exit);
  $pl->m("$host->{host}:", 'Collecting remote configuration');
  %remote_cfg = %{$host->config};
  $rmi = RObj->new($host->{host}, $host->{user}, $host->{ssh_key});
  $rmi->add_package('MysqlMasterInfo');
  $rmi->add_main(sub { MysqlMasterInfo->open(@_); });
  $master_info = $rmi->do($remote_cfg{'mysqld'}{'master-info-file'}
    || $remote_cfg{'mysqld'}{'datadir'} . '/master.info');
  $step++;
  exit_worker($pl, 1, $step) if($exit);

  # Save required databases
  $pl->m("$host->{host}:", 'Saving:', join(', ', @save_dbs));
  $rmi = $rmi->copy;
  $rmi->add_main(\&save_databases);
  $saved_dbs_file = $rmi->do($dry_run, \%remote_cfg,
    $save_user, $save_pass, @save_dbs);
  $step++;
  exit_worker($host, $pl, 1, $step) if($exit);

  $pl->m("$host->{host}:", 'Removing remote data');
  $rmi = $rmi->copy;
  $rmi->add_main(\&remove_datadir);
  $res = [$rmi->do($dry_run, $remote_cfg{'mysqld'}{'datadir'})]->[1];
  $pl->d("$host->{host}:", 'removed files:', @{@$res[0]} ? join("\n  ", @{@$res[0]}) : 'none' );
  $pl->e("$host->{host}:", 'got errors while removing files:', join("\n  ", @{@$res[1]})) if(@{@$res[1]});
  $step++;
  exit_worker($host, $pl, 1, $step) if($exit or @{@$res[1]});

  $pl->m("$host->{host}:", 'Copying data to remote');
  $res = copy_data($dry_run, $host->{host},
    $host->{user}, $host->{ssh_key}, $sandbox_path,
    $remote_cfg{'mysqld'}{'datadir'});
  $step++;
  exit_worker($host, $pl, 1, $step) if($exit or $res);

  return 0;
}

sub parse_hostref {
  my $ref = shift;
  my ($host, $user, $path);
  if($ref =~ /^(.+?)\@(.+?):(.+?)$/) {
    $host = $2;
    $user = $1;
    $path = $3;
  }
  elsif($ref =~ /^(.+?)\@(.+?)$/) {
    $user = $1;
    $host = $2;
    $path = undef;
  }
  elsif($ref =~ /^(.+?):(.+?)$/) {
    $host = $1;
    $path = $2;
  }
  elsif($ref =~ /^(.+?)$/) {
    $host = $1;
  }
  return undef if(!$host or $host eq '');
  return wantarray ? ($host, $user, $path) : [$user, $host, $path];
}

sub save_databases {
  my ($dry_run, $remote_cfg, $user, $pw, $dest, @databases) = @_;
  eval 'use File::Temp;';
  my $dumpfile = File::Temp->new( TEMPLATE => 'pdb-mm-XXXXXXXXXXXXXXX',
                                  DIR      => $dest,
                                  SUFFIX   => '.sql',
                                  UNLINK   => 0
                                );
  unless($dry_run) {
    system('mysqldump',
      '--skip-opt',
      '--user', $user,
      '--password='. $pw,
      '--socket', $remote_cfg->{'mysqld'}->{'socket'},
      '--add-drop-table',
      '--create-options',
      '--disable-keys',
      '--extended-insert',
      '--no-autocommit',
      '--quick',
      '--flush-privileges',
      '--databases',
      # Support testing by explicitly using the filename method.
      '--result-file', $dumpfile->filename,
      , join(' ', @databases)
    );
  }
  else {
    $? = -1;
  }

  if($? == -1 or ($? >> 8) > 0) {
    $dumpfile->unlink_on_destroy(1);
    return $?;
  }

  return $dumpfile->filename;
}

# Empties out the remote datadir, if !$dry_run
sub remove_datadir {
  my ($dry_run, $datadir_path) = @_;
  my ($results, $errors);
  eval "use File::Path qw(rmtree);";
  eval {
    unless($dry_run) {
      my %opts = ( 'keep_root' => 1, error => \$errors, result => \$results );
      rmtree($datadir_path, \%opts);
    }
    else {
      $results = [];
      $errors  = [];
    }
  };
  if($@) {
    R_die('DIE', $@);
  }
  return [$results, $errors];
}

# Copies sandbox data into the remote datadir, if !$dry_run
# Pre-condition mysql is not running, and all important data saved.
sub copy_data {
  my ($dry_run, $host, $user, $key, $sandbox_path, $datadir) = @_;
  unless($dry_run) {
    system('scp',
      '-B', '-C', '-r',
      '-q', '-p',
      $key ? ('-i', $key) : (),
      "$sandbox_path/data/",
      "$user\@$host". ':' ."$datadir/"
    );
  }
  else {
    $? = 0;
  }
  return $? >> 8;
}


=pod

=head1 NAME

pdb-master - build a master-master cluster from a mysqlsandbox

=head1 SYNOPSIS

pdb-master [options] <sandbox path> <[user@]host[:my.cnf path]>+

=head1 ARGUMENTS

After options, the arguments to pdb-master are a path to a sandbox.
This must be a filesystem path. Followed by one or more host references.
You may not mix the sandbox path, and host references. The tool will fail if
the first argument is not a directory.

Host references are an optional username followed by an at sign C<@>,
followed by a mandatory hostname, followed by an optional colon C<:> and
a path to a my.cnf file on the remote host.

Example host references:

C<root@db1> , C<db2:/etc/prd/my.cnf> , C<db3> , C<mysql@db4:/etc/my.cnf>

pdb-master tries to be as smart as possible in it's operation.
So, it will autodetect standard locations for RedHat and Debian Linux
distributions, and FreeBSD. Meaning that specifying the my.cnf path
is not usually needed.

Only the first two hosts are master-master the rest are built as query slaves hanging off the secondary master. This is because building master-master clusters with more than two masters is not well supported by mysql.

=head1 REQUIREMENTS

Where possible this tool inline's its requirements much like
L<http://code.google.com/p/maatkit/> in order to reduce external dependencies.

At the time of writing this tool needs the following external commands
available in the $PATH: ssh, mysql, mysqldump, scp, and tar.

For SSH access to the remote hosts, only public key authentication is allowed.
This is done both for security and to accomodate Debian. There is a Net::SSH::Perl
module but Debian refuses to package it because one of its dependencies is
"hard" to pacakge.

=head1 OPTIONS

=over 4

=item --help,-h

This help.

=item --dry-run,-n

Report on, but do not execute, actions that would be taken.

=item --ssh-key,-i

SSH key to use when connecting to remote hosts.
If not specified it is assumed that an L<ssh-agent(1)> is running,
or that .ssh/config has been setup appropriately.

Default: none

=item --save,-S

Preserve a database on the remote machine.
This option can be specified multiple times to save many databases.
The save/restore is done by doing a mysqldump and then reloading that mysqldump.

Default: mysql

=item --save-destination,-D

Where to save databases.

This parameter is a path to a directory on the remote machine that
has enough free space to save all databases specified with C<--save>.
The saving is done using C<mysqldump> on the remote machine.

Default: /tmp

=item --save-user

User for C<mysqldump> to use when saving databases. The user must exist
before the rebuild and after the rebuild. It's up to you to figure out how
to accomplish that.

Since the C<mysqldump> happens on the remote machine, this must be a user
that can access mysql from 'localhost', rather than the machine this tool
runs on.

Default: root

=item --save-password

Password for C<--save-user>.

Default: <none>

=item --repl-user

User to use/setup for replication. The user will be checked for existance,
and if it doesn't exist this tool will attempt to create the user

=back

=cut

1;
