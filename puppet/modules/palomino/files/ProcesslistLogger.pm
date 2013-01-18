# ProcesslistLogger.pm - a Perl module used in many PalominoDB Perl tools,
# that takes care of logging data about the tool to a specific database.
# Copyright (C) 2013 PalominoDB, Inc.
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

package ProcesslistLogger;

use strict;
use warnings FATAL => 'all';
use Data::Dumper;
$Data::Dumper::Indent = 2;

use constant ProcLogDebug => $ENV{PROCLOGDEBUG} || 0;

=pod

CREATE TABLE `process_list` (
  `Server` varchar(80) NOT NULL, /* Host that mk-loadavg is connected to */
  `EventTime` datetime NOT NULL, /* When this information was recorded */
  `ThrId` int(11) NOT NULL, /* Id column from show processlist */
  `User` varchar(16) NOT NULL,
  `Host` varchar(64) NOT NULL,
  `Db` varchar(64) DEFAULT NULL,
  `Command` enum('binlog dump','change user','close stmt','connect','connect out','create db','daemon','debug','delayed insert','drop db','drror','dxecute','fetch','field list','init db','kill','long data','ping','prepare','processlist','query','quit','refresh','register slave','reset stmt','set option','shutdown','sleep','statistics','table dump') DEFAULT NULL,
  `Time` int(11),
  `State` enum('after create','analyzing','checking permissions','checking table','cleaning up','closing tables','converting heap to myisam','copy to tmp table','copying to group table','copying to tmp table','copying to tmp table on disk','creating index','creating sort index','creating table','creating tmp table','deleting from main table','deleting from reference tables','discard_or_import_tablespace','end','executing','execution of init_command','freeing items','flushing tables','fulltext initialization','init','killed','locked','logging slow query','login','opening tables','opening table','preparing','purging old relay logs','query end','reading from net','removing duplicates','removing tmp table','rename','rename result table','reopen tables','repair by sorting','repair done','repair with keycache','rolling back','saving state','searching rows for update','sending data','setup','sorting for group','sorting for order','sorting index','sorting result','statistics','system lock','table lock','updating','updating main table','updating reference tables','user lock','user sleep','waiting for tables','waiting for table','waiting on cond','writing to net') DEFAULT NULL,
  `Info` text
/*  `Flagged` tinyint(1) TODO-ish: If this is true, then this is the query that caused the processlist to be saved. */
) ENGINE=InnoDB DEFAULT CHARSET=UTF8;
CREATE UNIQUE INDEX srv_evt_thr on `process_list` (`Server`, `EventTime`, `ThrId`);
CREATE INDEX user_host on `process_list` (`User`, `Host`);
CREATE INDEX cmd on `process_list` (`Command`);
/* CREATE INDEX cmd_state on `process_list` (`State`); */

=cut

sub new {
   my ($class, $args) = @_;
   if($args->{o}) {
      my $dp = DSNParser->new({key => 't', 'desc' => 'Table to log to', 'copy' => 0}, {key => 'i', 'desc' => 'Purge interval', 'copy' => 0}, {key => 'c', 'desc' => 'Cycles before data purge', 'copy' => 0});
      $args->{logdsn} = $dp->parse($args->{o});
      $args->{logdbh} = $dp->get_dbh($dp->get_cxn_params($dp->parse($args->{o})));
   }
   else {
      $args->{logdbh} = $args->{dbh};
   }
   $args->{logdsn}->{t} ||= 'process_list';
   if(not defined($args->{logdsn}->{i})) {
      $args->{logdsn}->{i} = 30;
   }
   $args->{logdbh}->{InactiveDestroy}  = 1;
   $args->{logdsn}->{c} ||= 100;
   $args->{cycles} = 0;
   $args->{sth} = $args->{logdbh}->prepare(
      "INSERT INTO `$args->{logdsn}->{D}`.`$args->{logdsn}->{t}` (Server, EventTime, ThrId, User, Host, Db, Command, Time, State, Info) ".
      "VALUES(". $args->{logdbh}->quote($args->{dsn}->{h}) .", FROM_UNIXTIME(?), ?, ?, ?, ?, ?, ?, ?, ?)"
   );
   bless $args, $class;
   return $args;
}

sub watch_event {
   my ($self, $watches) = @_;

   my $time_at = time;
   my $data = undef;
   foreach my $w (@$watches) {
      my ($wn, $opts) = split /:/, $w->{name}, 2;
      if($wn eq 'Processlist') {
         my @t = $w->{module}->get_last_data();
         $data = $t[0];
      }
   }
   $data = $self->{dbh}->selectall_arrayref("SHOW FULL PROCESSLIST", { Slice => {} }) unless($data);
   eval {
      foreach my $r (@$data) {
         ProcLogDebug && mk_loadavg::_d('Writing row'); 
         $self->{sth}->execute($time_at, $r->{Id}, $r->{User}, $r->{Host}, $r->{db}, lc($r->{Command}), $r->{Time}, lc($r->{State}), $r->{Info});
      }
      if($self->{cycles} > $self->{logdsn}->{c}) {
         ProcLogDebug && mk_loadavg::_d('Purging old rows');
         ProcLogDebug && mk_loadavg::_d("DELETE FROM `$self->{logdsn}->{D}`.`$self->{logdsn}->{t}` WHERE eventTime < NOW() - INTERVAL $self->{logdsn}->{i} DAY ORDER BY eventTime");
         $self->{logdbh}->do("DELETE FROM `$self->{logdsn}->{D}`.`$self->{logdsn}->{t}` WHERE eventTime < NOW() - INTERVAL $self->{logdsn}->{i} DAY ORDER BY eventTime");
         $self->{cycles} = 0;
      }
      else {
         $self->{cycles} += 1;
      }
      $self->{logdbh}->commit;
   };

   if($@) {
      warn "Transaction aborted because $@";
      eval { $self->{logdbh}->rollback };
   }
   return 1;
}

sub done {
   my $self = shift;
   ProcLogDebug && mk_loadavg::_d("Defragging processlog table.\n");
   $self->{logdbh}->do("OPTIMIZE TABLE `$self->{logdsn}->{D}`.`$self->{logdsn}->{t}`");
   $self->{logdbh}->disconnect() unless($self->{dbh} == $self->{logdbh});
   return 1;
}

sub set_dbh {
   my ($self, $dbh) = @_;
   ProcLogDebug && mk_loadavg::_d("Re-setting dbh");
   if($self->{logdbh} == $self->{dbh}) {
      $self->{dbh} = $dbh;
      $self->{logdbh} = $dbh;
      $self->{sth} = $dbh->prepare(
         "INSERT INTO `$self->{logdsn}->{D}`.`$self->{logdsn}->{t}` (Server, EventTime, ThrId, User, Host, Db, Command, Time, State, Info) ".
         "VALUES(". $self->{logdbh}->quote($self->{dsn}->{h}) .", FROM_UNIXTIME(?), ?, ?, ?, ?, ?, ?, ?, ?)"
      );
   }
   $self->{logdbh}->{InactiveDestroy}  = 1;
   return 1;
}

1;
