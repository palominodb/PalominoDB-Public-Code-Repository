# MmoveSlaves.pm
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
 
package MoveSlaves;
use strict;
use warnings FATAL => 'all';
use ProcessLog;
use Carp;
use DBI;
use DSN;
use MysqlSlave;
use FailoverPlugin;
use FailoverModule;
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
  my $self = shift;
  my $pdsn = $self->{'primary_dsn'};
  my $fdsn = $self->{'failover_dsn'};
  my ($pdbh, $fdbh);
  my $status = 1;
  FailoverPlugin->pre_verification_hook($pdsn, $fdsn, @{$self->{'slave'}});
  FailoverPlugin->begin_failover_hook($pdsn, $fdsn, @{$self->{'slave'}});
  $::PLOG->m('Connecting to:', join(', ', ($pdsn->get('h'), $fdsn->get('h'))));
  eval {
    ($pdbh, $fdbh) = ($pdsn->get_dbh(1), $fdsn->get_dbh(1));
  };
  if($@) {
    $::PLOG->e('Failover FAILED.');
    $::PLOG->e('DBI threw:', $@);
    FailoverPlugin->finish_failover_hook(0, $pdsn, $fdsn, @{$self->{'slave'}});
    FailoverPlugin->post_verification_hook(0, $pdsn, $fdsn, @{$self->{'slave'}});
    croak($@);
  }

  $::PLOG->m('Getting master/replication information.');
  my $pms = MysqlSlave->new($pdsn);
  my $fms = MysqlSlave->new($fdsn);
  my ($p_cur_binlog_file, $p_cur_binlog_pos) = $pms->master_status();
  my ($f_cur_binlog_file, $f_cur_binlog_pos) = $fms->master_status();
  my ($p_cur_binlog_num) = ($p_cur_binlog_file =~ /.*\.(\d+)$/);
  my $next_binlog_file = $p_cur_binlog_file;
  my $next_binlog_num = sprintf('%06d', $p_cur_binlog_num+1);
  $next_binlog_file =~ s/(.*\.)\d+$/$1$next_binlog_num/;
  $::PLOG->d('Master Binlog file:', $p_cur_binlog_file, ' position:', $p_cur_binlog_pos);
  $::PLOG->d('Master Binlog next file:', $next_binlog_file);

  # XXX There is a race condition here.
  # XXX If the master is *just* about to rotate its binlogs,
  # XXX the master could wind up rotating before we've had
  # XXX a chance to stop all the slaves.
  # XXX We can deal with this a couple of ways:
  # XXX 1) Do a global lock on the master for the few seconds
  # XXX    While we stop all the slaves and start them.
  # XXX    The advantage is that this one is guaranteed to work.
  # XXX    The downside is that it will briefly impact production.
  # XXX 2) Attempt to retry by picking a new logfile.
  # XXX    This is also race-y, but, there's less chance of the
  # XXX    same condition happening twice in a very short period of time.

  $::PLOG->m('Setting up slaves to stop.');
  foreach my $sl (($fdsn, @{$self->{'slave'}})) {
    my $ms = MysqlSlave->new($sl);
    $::PLOG->d('Stopping slave:', $sl->get('h'));
    $ms->stop_slave() unless($pretend);
    $::PLOG->d('Starting slave until: ', $next_binlog_file . '@4');
    $ms->start_slave($next_binlog_file, 4) unless($pretend);
  }
  $::PLOG->d('Flushing logs on:', $pdsn->get('h'));
  $pms->flush_logs() unless($pretend);

  ($f_cur_binlog_file, $f_cur_binlog_pos) = $fms->master_status();
  $::PLOG->d('Failover Binlog file:', $f_cur_binlog_file, ' position:', $f_cur_binlog_pos);
  $::PLOG->m('Moving slaves over.');
  foreach my $sl (@{$self->{'slave'}}) {
    my $ms = MysqlSlave->new($sl);
    # start slave until.. leaves the IO thread running
    $ms->stop_slave() unless($pretend);
    $ms->change_master_to(
      master_host => $fdsn->get('h'),
      master_log_file => $f_cur_binlog_file,
      master_log_pos  => $f_cur_binlog_pos
    ) unless($pretend);
    $ms->start_slave() unless($pretend);
    if(!$pretend) {
      my $i = 0;
      $status = 0;
      while($i < 3 and !$status) {
        $status = defined($ms->slave_status()->{'Seconds_Behind_Master'}) ? 1 : 0;
        sleep(1);
      }
      continue {
        $i++;
      }
      if(!$status) {
        $::PLOG->e('Slave not running on:', $sl->get('h'));
      }
    }
  }

  # Restart failover master replication to master
  $::PLOG->m('Restarting replication on failover master.');
  $fms->start_slave() unless($pretend);

  if(!$status) {
    $::PLOG->e('Failover FAILED');
    FailoverPlugin->finish_failover_hook(0, $pdsn, $fdsn, @{$self->{'slave'}});
    FailoverPlugin->post_verification_hook(0, $pdsn, $fdsn, @{$self->{'slave'}});
    croak('Failed to move slaves to failover master');
  }

  FailoverPlugin->finish_failover_hook(1, $pdsn, $fdsn, @{$self->{'slave'}});
  FailoverPlugin->post_verification_hook(1, $pdsn, $fdsn, @{$self->{'slave'}});

  return 0;
}

1;
