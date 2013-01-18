# ZRMBackup.pm
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

package ZRMBackup;
use strict;
use warnings FATAL => 'all';

use English qw(-no_match_vars);
use Data::Dumper;
use File::Spec;

use ProcessLog;

sub new {
  my ( $class, $pl, $backup_dir ) = @_;
  my $self = {};
  $self->{backup_dir} = ($backup_dir ? $backup_dir : $pl);
  bless $self, $class;

  unless( $self->_load_index() ) {
    return undef;
  }
  return $self;
}

# prevent AUTOLOAD from capturing this
sub DESTROY {}

# Returns the backup directory this ZRMBackup object represents
sub backup_dir {
  my ($self) = @_;
  return $self->{backup_dir};
}

# Returns a new ZRMBackup instance
# containing the previous backup's information.
# Can be used to walk back to a full backup for restore purposes.
sub open_last_backup {
  my ($self) = @_;
  if(not defined $self->last_backup) {
    die("No last backup present.\n");
  }
  return ZRMBackup->new(undef, $self->last_backup);
};

# Returns a list of all the backups back to the most recent full.
# Two optional parameters can be passed to manipulate the path
# to the last backup, in case the backup tree has been moved.
sub find_full {
  my ($self, $strip, $rel_base) = @_;
  my @backups;
  unshift @backups, $self;
  while($backups[0] && $backups[0]->backup_level != 0) {
    $::PL->d("unadjusted lookup:", $backups[0]->last_backup);
    my @path = File::Spec->splitdir($backups[0]->last_backup);
    my $path;
    if($strip and $strip =~ /^\d+$/) {
      for(my $i=0; $i<$strip; $i++) { shift @path; }
    }
    elsif($strip) {
      $_ = $backups[0]->last_backup;
      s/^$strip//;
      @path = File::Spec->splitdir($_);
    }
    if($rel_base) {
      unshift @path, $rel_base;
    }
    $path = File::Spec->catdir(@path);
    $::PL->d("adjusted lookup:", $path);
    unshift @backups, ZRMBackup->new(undef, $path);
  }
  # If a backup points to a non-existant
  # previous backup, then the loop above terminates
  # due to $backups[0] == undef
  # this shifts that off again.
  shift @backups unless($backups[0]);
  if($backups[0]->backup_level != 0) {
    croak('No full backup present in chain');
  }
  return @backups;
}

# Returns ($tar_return_code, $fh_of_tar_errors) in list context;
# and, $tar_return_code in scalar context.
# If there was an error executing tar for some reason,
# then the return code will be undef.
#
# Extract this backup to the specified directory.
# Requires tar to be in path.
sub extract_to {
  my ($self, $xdir) = @_;
  my @args = ();
  if($self->compress =~ /gzip/) {
    @args = ("tar","-xzf", $self->backup_dir . "/backup-data", "-C", $xdir);
  }
  elsif($self->compress =~ /bzip2/) {
     @args = ("tar","-xjf", $self->backup_dir . "/backup-data", "-C", $xdir);
  }
  else {
    # hope that the tool accepts the -d like needed by MySQL-zrm
    # and that the tool exists on the current machine
    @args = ($self->compress ." -d ". $self->backup_dir ."/backup-data". " | tar -C $xdir -xf -");
  }
  my $r = $::PL->x(sub { system(@_) }, @args);
  return wantarray ? ($r->{rcode}, $r->{fh}) : $r->{rcode};
}

## Example indexes
# backup-set=vty02
# backup-date=20100226124001
# mysql-server-os=Linux/Unix
# backup-type=regular
# host=db04
# backup-date-epoch=1267216801
# retention-policy=2W
# mysql-zrm-version=ZRM for MySQL Community Edition - version 2.1
# mysql-version=5.1.34-log
# backup-directory=/backups/vty02/20100226124001
# backup-level=1
# replication=master.info relay-log.info
# incremental=mysql-bin.[0-9]*
# next-binlog=mysql-bin.001958
# last-backup=/backups/vty02/20100226084001
# /backups/vty02/20100226124001/master.info=51fed0d70ab28254380e8416cc210ae0
# /backups/vty02/20100226124001/mysql-bin.001957=1e2ac040fa05c82842d2a94e465f2fdd
# /backups/vty02/20100226124001/relay-log.info=6c90fbf5e00ae833ab2ca0305185dc6c
# backup-size=3.50 MB
# compress=/usr/local/bin/gzip_fast.sh
# backup-size-compressed=0.55 MB
# read-locks-time=00:00:00
# flush-logs-time=00:00:00
# compress-encrypt-time=00:00:00
# backup-time=00:00:02
# backup-status=Backup succeeded
#
#
# backup-set=c2
# backup-date=20100226124502
# mysql-server-os=Linux/Unix
# host=c2s
# backup-date-epoch=1267217102
# retention-policy=2W
# mysql-zrm-version=ZRM for MySQL Community Edition - version 2.0
# mysql-version=5.0.84-percona-highperf-b18-log
# backup-directory=/bk1/backups/c2/20100226124502
# backup-level=1
# incremental=mysql-bin.[0-9]*
# next-binlog=mysql-bin.031414
# last-backup=/bk1/backups/c2/20100226084502
# backup-size=619.61 MB
# compress=/usr/local/bin/gzip_fast.sh
# backup-size-compressed=108.40 MB
# read-locks-time=00:00:00
# flush-logs-time=00:00:00
# compress-encrypt-time=00:03:20
# backup-time=00:01:08
# backup-status=Backup succeeded
#

sub _load_index() {
  my ($self) = @_;
  my $fIdx;
  unless(open $fIdx, "<$self->{backup_dir}/index") {
    return undef;
  }
  $self->{idx} = ();
  while(<$fIdx>) {
    # Newlines will screw up later transformations.
    chomp;
    next if $_ eq ""; # Skip empty lines.
    my ($k, $v) = split(/=/, $_, 2);
    next if ($k eq "");
    $k =~ s/-/_/g;
    next if $k =~ /\//; # File lists are useless to us right now.
    # Convert backup sizes to kilobytes
    # kilos are easier to work with, usually.
    if($k eq "backup_size" or $k eq "backup_size_compressed") {
      if($v =~ / MB$/) {
        $v =~ s/ MB$//;
        $v *= 1024;
      }
      elsif($v =~ / GB$/) {
        $v =~ s/ GB$//;
        $v *= 1024;
        $v *= 1024;
      }
    }
    # Normalize backup_status to something more easily tested.
    elsif($k eq "backup_status") {
      if($v eq "Backup succeeded") {
        $v = 1;
      }
      else {
        $v = 0;
      }
    }
    # Convet time keys to seconds for easier manipulation.
    elsif($k =~ /_time$/) {
      my ($h, $m, $s) = split(/:/, $v);
      $v  = $h*3600;
      $v += $m*60;
      $v += $s;
    }
    # Make these a real array.
    elsif($k eq "raw_databases_snapshot" or $k eq "replication") {
      my @t = split(/\s+/, $v);
      $v = \@t;
    }
    $self->{idx}{$k} = $v;
  }
  return 1;
}

# Expose $self->{idx}{$name} as $bk_inst->$name
# For convienience, and, since, this thing is just a wrapper
# around that data.
our $AUTOLOAD;
sub AUTOLOAD {
  my ($self) = @_;
  my $name = $AUTOLOAD;
  $name =~ s/.*:://;
  ProcessLog::_PdbDEBUG >= ProcessLog::Level2
    && $::PL->d("AUTOLOAD:", $name, '->', $self->{idx}{$name});
  if(exists $self->{idx}{$name}) {
    return $self->{idx}{$name};
  }
  return undef;
};

1;
