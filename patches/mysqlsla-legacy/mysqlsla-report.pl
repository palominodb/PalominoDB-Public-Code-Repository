#!/usr/bin/perl -w
# vim:fenc=utf-8:ts=4:sw=4:et

# SQLProfiler v0.1 modifications to mysqlsla, based on mysqlsla v1.8 DEBUG Apr 17 2008
#
# Modifications Copyright (c) 2008-2013 PalominoDB, Inc.
# Modifications fall under the original mysqlsla GPLv2 license.  Original work
# copyrights below.
#
# mysqlsla v1.8 DEBUG Apr 17 2008
# http://hackmysql.com/mysqlsla

# mysqlsla (MySQL Statement Log Analyzer) analyzes slow, general, and raw MySQL query logs.
# Copyright 2007-2008 Daniel Nichter
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# The GNU General Public License is available at:
# http://www.gnu.org/copyleft/gpl.html

use strict;
use warnings FATAL => 'all';
use POSIX qw(mktime);
use Time::HiRes qw(gettimeofday tv_interval);
use DBI;
use Getopt::Long;
eval { require Term::ReadKey; };
use Data::Dumper;
use Mail::Mailer;

my $RK = ($@ ? 0 : 1);

$|=1;

my $WIN = ($^O eq 'MSWin32' ? 1 : 0);
my %op;
my %mycnf; # ~/.my.cnf
my ($dbh, $query);
my ($dbName,$dbUser,$dbPass,$dbHost,$dbDatabase,$dbPort,$dbSocket);
my ($profilerDbh, $profilerQuery);
my ($profilerName,$profilerUser,$profilerPass,$profilerHost,$profilerDatabase,$profilerPort,$profilerSocket);
my (@q_a, %q_h);
my ($stmt, $q); # Used by parse_ and abstract_stmt subs
my $total_queries;
my %slow_users;
my ($t0, $t1, $t, @t);
my %filter = qw(DELETE 0 DO 0 INSERT 0 REPLACE 0 SELECT 0 TRUNCATE 0 UPDATE 0 USE 0 CALL 0 SET 0 START 0 SHOW 0 ROLLBACK 0 COMMIT 0 CHANGE 0 DROP 0 RESET 0);
my %sort = qw(c 1 rp 1 rr 1 e 1 ce 1 t 2 at 2 l 2 al 2 rs 2 re 2 rs_max 2 re_max 2);
my %dbs;
my %isolate; # Used by --only-* options
my $need_examples;
my $res;  # Set by mnp sub
my %logTypes = (GENERAL => 0, SLOW => 1, RAW => 2, UNKNOWN => 127);
my @logTypeNames = ('general', 'slow', 'raw');
my $logStart;
my $logEnd;
my $fromAddress = 'mysql@localhost';


GetOptions(
   \%op,
# "user=s",
# "password:s",
# "host=s",
# "port=s",
# "socket=s",
   "no-mycnf",
   "db|databases=s",
   "dsn|D=s",
   "help|?",
   "general|g=s",
   "slow|s=s",
   "raw|r=s",
   "flat",
   "examples",
   "milliseconds",
   "sort=s",
   "flush-qc",
   "avg|n=i",
   "percent",
   "top=n",
   "filter=s",
   "grep=s",
   "mp=i",
   "np=i",
   "only-databases=s",
   "only-users=s",
   "only-hosts=s",
   "only-ids=s",
   "pd|distribution",
   "nthp|nth-percent=i",
   "pq|print-queries",
   "ex|explain",
   "te|time-each-query",
   "ta|time-all-queries",
   "nr|no-report",
   "profiler=s",
   "email|email-report=s",
   "debug",
   "last=s"
);


if((!$op{general} && !$op{slow} && !$op{raw} && !$op{email}) || $op{help})
{
   show_help_and_exit();
}

if ($op{email} && !$op{profiler}) {
    print "Email mode requires Profiler mode.\n";
    show_help_and_exit();
}
option_sanity_check();

if ($op{dsn}) {
    ($dbName,$dbUser,$dbPass,$dbHost,$dbDatabase,$dbPort,$dbSocket) = parse_dsn($op{dsn});

    get_user_mycnf() unless $op{'no-mycnf'};

    $dbHost     = $mycnf{host}      unless $dbHost;
    $dbPort     = $mycnf{port}      unless $dbPort;
    $dbSocket   = $mycnf{socket}    unless $dbSocket;
    $dbUser     = $mycnf{user}      unless $dbUser;
    
    $dbUser     = $ENV{USER}        unless $dbUser;
}

# Command line options override ~/.my.cnf
# $mycnf{host}   = $op{host}   if $op{host};
# $mycnf{port}   = $op{port}   if $op{port};
# $mycnf{socket} = $op{socket} if $op{socket};
# $mycnf{user}   = $op{user}   if $op{user};
# 
# $mycnf{user} ||= $ENV{USER};

# Default values
$op{avg}  ||= 1;
$op{top}  ||= 10;
$op{mp}   ||= 5;
$op{np}   ||= 10;
$op{nthp} ||= 95;

$op{last} = '1d' unless exists $op{last};

if(($op{te} || $op{ta}))
{
   if(!$op{filter})
   {
      print "Safety for time-each/time-all safety is enabled.\n";
      $op{filter} = "-*,+SELECT,+USE";
   }
   else
   {
      print "Safety for time-each/time-all safety is DISABLED!\n";
   }
}

if($op{filter}) { set_filter(); }

if($op{flat} && $op{examples}) { $op{flat} = 0; }

if($op{'only-databases'})
{
   isolate_x('databases');
   $op{db} = $op{'only-databases'};
}
if($op{'only-users'}) { isolate_x('users'); }
if($op{'only-hosts'}) { isolate_x('hosts'); }
if($op{'only-ids'})   { isolate_x('ids');   }


d("Before defined(dbPass)");
if(defined($dbPass))
{
   if($dbPass eq '') # Prompt for password
   {
    d("Before Term::ReadKey::ReadMode(2)");
      Term::ReadKey::ReadMode(2) if $RK;
      print "Password for database user $dbUser: ";
      chomp($dbPass = <STDIN>);
      Term::ReadKey::ReadMode(0), print "\n" if $RK;
    d("After Term::ReadKey::ReadMode(0)");
   }
}

# Connect to MySQL
if( $op{ex} || $op{te} || $op{ta} || 
    ($op{sort} && ($op{sort} eq 'rp' || $op{sort} eq 'rr' || $op{sort} eq 'e' || $op{sort} eq 'ce'))
  )
{
    my $dsn;

    if($dbSocket && -S $dbSocket) {
        $dsn = "DBI:mysql:mysql_socket=$dbSocket";
    } elsif($dbHost) {
        $dsn = "DBI:mysql:host=$dbHost" . ($dbPort ? ";port=$dbPort" : "");
    } else {
        $dsn = "DBI:mysql:host=localhost";
    }

    if($op{debug}) {
        print "DBI DSN: $dsn\n";
    }

    d("About to connect to db for EXPLAINs etc.");
    $dbh = DBI->connect($dsn, $dbUser, $dbPass, { PrintError => 0 });
    if($DBI::err)
    {
       print "Cannot connect to MySQL.\n";
       print "MySQL error message: $DBI::errstr\n";
       exit;
    }
    d("Done connecting to db for EXPLAINs etc.");
}

if ($op{profiler}) {
    ($profilerName,$profilerUser,$profilerPass,$profilerHost,
     $profilerDatabase,$profilerPort,$profilerSocket) = parse_dsn($op{profiler});
    my $dbiDsn;
    
    if ($profilerSocket && -S $profilerSocket) {
        $dbiDsn = "DBI:mysql:mysql_socket=$profilerSocket";
    } elsif ($profilerHost) {
        $dbiDsn = "DBI:mysql:host=$profilerHost" . ($profilerPort ? ";port=$profilerPort" : "");
    } else {
        $dbiDsn = "DBI:mysql:host=localhost";
    }
    
    d("About to connect to db for profiler; dsn: $dbiDsn");
    $profilerDbh = DBI->connect($dbiDsn, $profilerUser, $profilerPass, { PrintError => 0 });
    if($DBI::err)
    {
       print "Cannot connect to MySQL.\n";
       print "MySQL error message: $DBI::errstr\n";
       exit;
    }
    d("Done connecting to db for profiler");
}


$op{'sort'} ||= ($op{slow} ? 't' : 'c');

if($op{examples}     ||
   $op{ex}           ||
   $op{te}           ||
   $op{ta}           ||
   $op{sort} eq 'rp' ||
   $op{sort} eq 'rr' ||
   $op{sort} eq 'e'  ||
   $op{sort} eq 'ce'
  )
{
   $need_examples = 1;
}
else { $need_examples = 0; }

# Build @q_a and/or %q_h from log files
parse_logs();

$total_queries = 0;
if($op{ta} || $op{pq}) { $total_queries = scalar @q_a; }
else { for(keys %q_h) { $total_queries += $q_h{$_}->{c}; } }

print "$total_queries total queries, " , scalar keys %q_h , " unique.\n";

exit if (!$total_queries and !$op{email});

if($op{db})
{
   print "Databases for Unknown: ";
   for(split ',', $op{db})
   {
      print "$_ ";
      $dbs{$_} = 1;
   }
   print "\n";
}

print "grep pattern: \'$op{grep}\'\n" if $op{grep};
print "Sorting by '$op{sort}'.\n";

if($op{'flush-qc'})
{
   $dbh->do("FLUSH QUERY CACHE;");
   print "Flushed query cache.";
}

print_queries()    if $op{pq};
time_all_queries() if $op{ta};

if ($op{profiler}) {
    if ($op{email}) {
        do_email_report();
    } else {
        do_table_insert();        
    }
} elsif (!$op{nr}) {
    do_reports();
}

exit;


#
# Subroutines
#

sub show_help_and_exit
{

   print <<"HELP";
mysqlsla v1.8 DEBUG Apr 17 2008
MySQL Statement Log Analyzer analyzes slow, general, and raw MySQL query logs.

Command line options (abbreviations work):
   --dsn DSN        Connect to the database described by a DSN of
                    the format:
                        u=user      Username for MySQL connection
                        p=passwd    Password for MySQL connection
                        h=hostname  Hostname or IP address of MySQL
                                    host
                        D=dbname    Database name (not used)
                        P=port      Port number of MySQL server
                        S=/path     Path to UNIX domain socket, for
                                    non-TCP/IP connections; mutually
                                    exclusive with h=,P=
                        N=Descriptive Name
                                    Descriptive name for this DSN; only
                                    used for SQL Profiler mode (don't forget
                                    quoting!)
   --profiler DSN   Run in SQL Profiler mode; results are stored in tables
                        in the database identified by the passed DSN (same
                        format as --dsn).  Profiler results need not be stored
                        in the same MySQL instance as referred to by --dsn.
   --email-report ADDR
                    Run in email reporting mode.  Requires SQL Profiler mode.
                        When run in this mode, the script will email a report to
                        the specified email address.  The --top and --sort modes
                        function as you would expect in this mode.
   --last TIMESPEC  Report on jobs run in the last TIMESPEC.  TIMESPEC is a
                        number followed by a modifier; accepted modifiers include
                        's' (seconds), 'm' (minutes), 'h' (hours), 'd' (days), 'w'
                        (weeks), 'M' (months), and 'y' (years).  Examples:
                        1d      One day
                        13h     13 hours
                        3M17m   3 months, 17 minutes
   --no-mycnf       Don't read ~/.my.cnf
   --help           Prints this
   --debug          Print debug information
   --general LOG    Read queries from general LOG | These options are mutually
   --slow LOG       Read queries from slow LOG    | exclusive. Multiple logs
   --raw LOG        Read queries from LOG         | can be given like
                                                  | file1;file2;...
   --flat           Don't capitalize key words in abstracted SQL statements
   --examples       Show example queries, not abstracted SQL statements
   --milliseconds   Show query and lock times in milliseconds if < 1 second

   --print-queries     Print all valid queries from all logs
   --explain           EXPLAIN a sample of each unique query
   --time-each-query   Time a sample of each unique query executed individually
   --time-all-queries  Time all queries executed in sequence (general/raw logs)

   --sort VALUE     Sort queries in descending order by VALUE:
                    With --general or --raw:
                       c    (count--default),
                       rp   (rows produced from EXPLAIN),
                       rr   (rows read from EXPLAIN),
                       e    (query execution time)
                       ce   (aprox. total query execution time {c * e})
                    With --slow:
                       The above VALUES (c, rp, rr, e, ce) and,
                       t      (total query time--default)
                       at     (average query time)
                       l      (total lock time)
                       al     (average lock time)
                       rs     (average rows sent)
                       rs_max (max rows sent)
                       re     (average rows examined)
                       re_max (max rows examined)

   --avg N          Average execution time over N runs (default 1)
   --databases D    Try using databases D for queries with Unknown database
   --distribution   Show distribution of slow times (not show by default)
   --filter S       Allow (+)/discard (-) statements S (default -*,+SELECT,+USE)
   --flush-qc       Execute a FLUSH QUERY CACHE; before timing queries
   --grep P         grep for statements that match Perl regex pattern P
   --mp N           Don't show distributions less then N percent (default 5)
   --no-report      Don't show usual report (useful with --time-all-queries)
   --np N           Show at most N percentage distributions (default 10)
   --nth-percent N  Show Nth percentage for slow times (default 95) 
   --only-databases X  Analyze only queries using databases X
   --only-users X      Analyze only queries belonging to users X
   --only-hosts X      Analyze only queries belonging to hosts X
   --only-ids X        Analyze only queries belonging to connection IDs X
   --percent        Don't count each time run, show percentage complete
   --top N          Show only the top N queries (default 10)

Visit http://hackmysql.com/mysqlsla for a lot more information.
HELP

   exit;
}

sub get_user_mycnf
{
   return if $WIN;
   open MYCNF, "$ENV{HOME}/.my.cnf" or return;
   while(<MYCNF>)
   {
      if(/^(.+?)\s*=\s*"?(.+?)"?\s*$/)
      {
         $mycnf{$1} = $2;
         print "get_user_mycnf: read '$1 = $2'\n" if $op{debug};
      }
   }
   $mycnf{'pass'} ||= $mycnf{'password'} if exists $mycnf{'password'};
   close MYCNF;
}

sub option_sanity_check
{
   goto OPT_ERR if ($op{general} && ($op{slow}    || $op{raw}));
   goto OPT_ERR if ($op{slow}    && ($op{general} || $op{raw}));
   goto OPT_ERR if ($op{raw}     && ($op{slow}    || $op{general}));

   if($op{sort})
   {
      if(!exists $sort{$op{sort}})
      {
         print "Invalid sort option '$op{sort}'. \n";
         exit;
      }

      if(($op{general} || $op{raw}) && $sort{$op{sort}} != 1)
      {
         print "Cannot sort by '$op{sort}' with --general or --raw.\n";
         exit;
      }
   }

   if($op{ta} && $op{slow})
   {
      print "Option --time-all-queries only works with general and raw logs.\n";
      exit;
   }

   if($op{'only-ids'} && ($op{slow} || $op{raw}))
   {
      print "Option --only-ids only works with general logs.\n";
      exit;
   }
   if($op{'only-databases'} && $op{slow})
   {
      print "Option --only-databases only works with general and raw logs.\n";
      exit;
   }
   if(($op{'only-users'} || $op{'only-hosts'}) && $op{raw})
   {
      print "Options --only-users and --only-hosts only work with general and slow logs.\n";
      exit;
   }

   return;

   OPT_ERR:
      print "Options --general, --slow, and --raw are mutually exclusive.\n";
   exit;
}

sub parse_logs 
{
   my @l;

   if($op{general}) { @l = split ',', $op{general}; parse_general_logs(@l); }
   if($op{slow})    { @l = split ',', $op{slow};    parse_slow_logs(@l);    }
   if($op{raw})     { @l = split ',', $op{raw};     parse_raw_logs(@l);     }
}

sub parse_general_logs {
   my @logs = @_;
   my $valid_stmt;
   my $have_stmt;
   my $match;
   my %use_db;
   my %users;
   my %hosts;
   my $cid;
   my $cmd;
   my $dateStamp;
   my $timeStamp;
   my @timeBits;

   for(@logs)
   {
      open LOG, "< $_" or warn "Couldn't open general log '$_': $!\n" and next;
      print "Reading general log '$_'.\n";

      $valid_stmt = 0;
      $have_stmt  = 0;
      $match      = '';
      $use_db{0}  = '';
      $users{0}   = '';
      $hosts{0}   = '';
      $cid        = 0;
      $cmd        = '';
      $dateStamp  = '';
      $timeStamp  = '';
      
      while(<LOG>)
      {
         next if /^\s*$/;

         if(!$have_stmt)
         {
            next unless /^[\s\d:]+(Query|Execute|Connect|Init|Change)/;

            if(/^\s+(\d+) (Query|Execute|Connect|Init|Change)/) {
                $cid = $1;
                $cmd = $2;
            } elsif(/^(\d{6})\s+([\d:]+)\s+(\d+) (Query|Execute|Connect|Init|Change)/) {
                $dateStamp = $1;
                $timeStamp = $2;
                $cid = $3;
                $cmd = $4;
                
                @timeBits = split(/:/, $timeStamp);
                $logEnd = mktime(
                    $timeBits[2],
                    $timeBits[1],
                    $timeBits[0],
                    substr($dateStamp, -2),
                    substr($dateStamp, -4, 2) - 1,
                    (substr($dateStamp, 0, 2) < 50 ? substr($dateStamp, 0, 2) + 100 : substr($dateStamp, 0, 2))
                );
                if (! defined($logStart)) {
                    $logStart = $logEnd;
                }
            } else {
               d("parse_general_logs: FALSE-POSITIVE MATCH: $_"); # D
               next;
            }

            $users{$cid}  = '?' if !exists $users{$cid};
            $hosts{$cid}  = '?' if !exists $hosts{$cid};
            $use_db{$cid} = 0   if !exists $use_db{$cid};

            d("parse_general_logs: cid $cid, cmd $cmd"); # D

            if($cmd eq "Connect")
            {
               if(/Connect\s+(.+) on (\w*)/)
               {}
               elsif(/Connect\s+(.+)/)
               {}
               else
               {
                  d("parse_general_logs: FALSE-POSITIVE Connect MATCH: $_"); # D
                  next;
               }

               if($1 ne "")
               {
                  if($1 =~ /^Access/)  # Ignore "Access denied for user ..."
                  {
                     d("parse_general_logs: ignoring: $_"); # D
                     next;
                  }

                  my @x = split('@', $1);
                  $users{$cid} = $x[0];
                  $hosts{$cid} = $x[1];
               }
               
               if($2 && $2 ne "")
               {
                  $use_db{$cid} = $2;
                  push @q_a, "USE $use_db{$cid};" if ($op{ta} || $op{pq});
               }

               d("parse_general_logs: Connect $users{$cid}\@$hosts{$cid} db $use_db{$cid}"); # D

               next;
            }

            if($cmd eq "Init")
            {
               /Init DB\s+(\w+)/;
               $use_db{$cid} = $1;
               push @q_a, "USE $use_db{$cid};" if ($op{ta} || $op{pq});
               d("parse_general_logs: cid $cid, Init DB $use_db{$cid}"); # D
               next;
            }

            if($cmd eq "Change")
            {
               /Change user\s+(.+) on (\w*)/;

               my $old_cid_info = "$users{$cid}\@$hosts{$cid} db $use_db{$cid}";

               if($1 ne "")
               {
                  my @x = split('@', $1);
                  $users{$cid} = $x[0];
                  $hosts{$cid} = $x[1];
               }

               if($2 ne "")
               {
                  $use_db{$cid} = $2;
                  push @q_a, "USE $use_db{$cid};" if ($op{ta} || $op{pq});
               }

               d("parse_general_logs: cid $cid CHANGE old:$old_cid_info > new:$users{$cid}\@$hosts{$cid} db $use_db{$cid}"); # D

               next;
            }

            $have_stmt = 1;

            if($cmd eq "Query")      { /Query\s+(.+)/;             $match = $1; }
            elsif($cmd eq "Execute") { /Execute\s+\[\d+\]\s+(.+)/; $match = $1; }

            $stmt = $match . "\n";
            $stmt =~ /^(\w+)/;

            $valid_stmt = 1;

            if(! (exists $filter{uc $1} && !$filter{uc $1}) )
            {
               $valid_stmt = 0;
               d("parse_general_logs: stmt FAILS filter"); # D
            }
            elsif($op{'only-ids'} && !exists $isolate{'ids'}->{$cid})
            {
               $valid_stmt = 0;
               d("parse_general_logs: stmt FAILS only-ids"); # D
            }
            elsif($op{'only-databases'} && !exists $isolate{databases}->{$use_db{$cid}})
            {
               $valid_stmt = 0;
               d("parse_general_logs: stmt FAILS only-databases"); # D
            }
            elsif($op{'only-users'} && !exists $isolate{users}->{$users{$cid}})
            {
               $valid_stmt = 0;
               d("parse_general_logs: stmt FAILS only-users"); # D
            }
            elsif($op{'only-hosts'} && !exists $isolate{hosts}->{$hosts{$cid}})
            {
               $valid_stmt = 0;
               d("parse_general_logs: stmt FAILS only-hosts"); # D
            }

            d("parse_general_logs: have_stmt $have_stmt, valid_stmt $valid_stmt, cid $cid, db $use_db{$cid} : matched '$stmt'"); # D
         }
         else
         {
            if(/^[\s\d:]+\d [A-Z]/)  # New CMD so the stmt we have now is done
            {
               d("parse_general_logs: have_stmt $have_stmt, valid_stmt $valid_stmt : NEW stmt"); # D

               $have_stmt = 0;

               if($valid_stmt)
               {
                  if($op{grep} && ($stmt !~ /$op{grep}/io))
                  {
                     $valid_stmt = 0;
                     d("parse_general_logs: previous stmt FAILS grep"); # D
                  }

                  if($valid_stmt)
                  {
                     if($op{ta} || $op{pq}) { push @q_a, $stmt; }

                     if(!$op{nr})
                     {
                        abstract_stmt(); # Sets $q to abstracted form of $stmt

                        my $x = $q_h{$q} ||= { };

                        if($need_examples)
                        {
                           $x->{sample} = $stmt;
                        }

                        $x->{c} += 1;
                        $x->{db} = $use_db{$cid} if $x->{c} == 1;
                        # TODO : what if db changes due to a Change user cmd?

                        d("parse_general_logs: c $x->{c}, cid $cid, db $x->{db} : SAVED previous stmt '$stmt'"); # D
                     }
                  }
               }
               else { d("parse_general_logs: valid_stmt $valid_stmt : previous stmt INVALID"); } # D

               redo;
            }
            else { $stmt .= $_ unless !$valid_stmt; }
         }
      }
      close LOG;
   }
}

sub parse_slow_logs
{
   my @logs = @_;
   my ($valid_stmt, $n_stmts);
   my ($user, $host, $IP);
   my ($time, $lock, $rows_sent, $rows_examined);
   my $use_db;
   my $timeStamp;
   my $dateStamp;

   for(@logs)
   {
      open LOG, "< $_" or warn "Couldn't open slow log '$_': $!\n" and next;
      print "Reading slow log '$_'.\n";

      while(<LOG>)
      {
         last if !defined $_;
         
         if (/^# Time: (\d+) ([\d:]+)/) {
             $dateStamp = $1;
             $timeStamp = $2;
             $logEnd = mktime(
                 substr($timeStamp, -2),
                 substr($timeStamp, -5, 2) - 1,
                 substr($timeStamp, 0, 2),
                 substr($dateStamp, -2),
                 substr($dateStamp, -4, 2) - 1,
                 (substr($dateStamp, 0, 2) < 50 ? substr($dateStamp, 0, 2) + 100 : substr($dateStamp, 0, 2))
             );
             
             if (! defined($logStart)) {
                 $logStart = $logEnd;
             }
         }
         
         next until /^# User/;

         ($user, $host, $IP) =
            /^# User\@Host: (.+?) \@ (.*?) \[(.*?)\]/ ? ($1,$2,$3) : ('','','');

         d("parse_slow_logs: header '$_'");

         $user =~ s/(\w+)\[\w+\]/$1/;

         if($op{'only-users'} && !exists $isolate{users}->{$user})
         {
            d("parse_slow_logs: stmt FAILS only-users"); # D
            next;
         }
         if($op{'only-hosts'} &&
                  (!exists $isolate{hosts}->{$host} && !exists $isolate{hosts}->{$IP}))
         {
            d("parse_slow_logs: stmt FAILS only-hosts"); # D
            next;
         }

         AGAIN: $_ = <LOG>;
         d("parse_slow_logs: header '$_'");
         if(/^# Query_time/)
         {
            ($time, $lock, $rows_sent, $rows_examined) =
               /^# Query_time: (.*?)\s+Lock_time: (.*?)\s+Rows_sent: (\d+)\s+Rows_examined: (\d+)/;
            # Some bad queries have their query time set very high ( > 2^32s which is many, many, days ).
            # Skip those.
            if($time >= 2**28)
            {
               d("parse_slow_logs: stmt FAILS $time < ". 2**28);
               next;
            }
         }
         elsif(/^# Thread_id/) {
             ($use_db) =
                /^# Thread_id: \d+  Schema: (.+?)/;
             goto AGAIN;
         }


         $stmt = '';

         while(<LOG>)
         {
            last if /^#/;
            last if /^\/(?!\*)/;  # skip log header lines but not SQL comment lines
            next if /^\s*$/;

            $stmt .= $_;
         }

         chomp $stmt;

         next if( $stmt eq '' ); # Empty statement is boring.

         $valid_stmt = 0;
         $use_db     = '';

         d("parse_slow_logs: v = $valid_stmt, read stmt '$stmt'"); # D

         # Check for compound statements
         $n_stmts = 1;
         $n_stmts++ while $stmt =~ /;\n/g;

         if($n_stmts > 1)
         {
            d("parse_slow_logs: v = $valid_stmt, compound stmt"); # D

            my @s = split(/;\n/, $stmt);
            my $grep_matches = 0;

            for(@s)
            {
               $_ .= ";\n" if $_ !~ /;\s*$/; # Put ; back that split removed

               /^\s*(\w+)/;
               $q = $1;

               if(lc($1) eq "use")
               {
                  /use (\w+)/i;
                  $use_db = $1;
                  $_ = '';
               }
               else
               {
                  if(! (exists $filter{uc $1} && !$filter{uc $1}) )
                  {
                     $_ = '';
                     d("parse_slow_log: part of compound stmt FAILS filter ($1)"); # D
                  }
                  if($op{grep} && ($_ =~ /$op{grep}/io)) { $grep_matches = 1; }
               }
            }

            if(!$op{grep} || ($op{grep} && $grep_matches))
            {
               $stmt = join '', @s;
               $valid_stmt = 1 if $stmt ne '';
            }
         }
         else
         {
            $valid_stmt = 1;

            $stmt =~ /^\s*#?\s*(\w+)/;
            $q = $1;

            if(! (exists $filter{uc $1} && !$filter{uc $1}) )
            {
               $valid_stmt = 0;
               d("parse_slow_log: stmt FAILS filter ($1)"); # D
            }
            if($op{grep} && ($stmt !~ /$op{grep}/io))   { $valid_stmt = 0; }
         }

         if($valid_stmt)
         {
            if($op{ta} || $op{pq}) { push @q_a, $stmt; }

            if(!$op{nr})
            {
               abstract_stmt(); # Sets $q to abstracted form of $stmt

               my $x = $q_h{$q} ||= { t_min  => $time,
                                      t_max  => $time,
                                      l_min  => $lock,
                                      l_max  => $lock,
                                      rs_min => $rows_sent,
                                      rs_max => $rows_sent,
                                      re_min => $rows_examined,
                                      re_max => $rows_examined,
                                      db     => 0,
                                      user   => "$user\@$host/$IP"
                                    };

               if($need_examples)
               {
                  $x->{sample} = $stmt;
               }

               # Totals and averages
               $x->{c}  += 1;
               $x->{t}  += $time;
               $x->{l}  += $lock;
               $x->{rs} += $rows_sent;
               $x->{re} += $rows_examined;

               # Distribution of values
               push @{$x->{t_a}}, $time;
               push @{$x->{l_a}}, $lock;

               # min-max values
               $x->{t_min}  = $time if $time < $x->{t_min};
               $x->{t_max}  = $time if $time > $x->{t_max};
               $x->{l_min}  = $lock if $lock < $x->{l_min};
               $x->{l_max}  = $lock if $lock > $x->{l_max};
               $x->{rs_min} = $rows_sent if $rows_sent < $x->{rs_min};
               $x->{rs_max} = $rows_sent if $rows_sent > $x->{rs_max};
               $x->{re_min} = $rows_examined if $rows_examined < $x->{re_min};
               $x->{re_max} = $rows_examined if $rows_examined > $x->{re_max};

               $slow_users{$x->{user}} += 1;

               $x->{db} = $use_db if !$x->{db};

               d("parse_slow_logs: c = $x->{c}, db = $x->{db}, SAVED stmt '$stmt'"); # D
            }
            else { d("parse_slow_logs: v = $valid_stmt, INVALID stmt (fails filter or grep)"); } # D
         }

         redo;
      }
      close LOG;
   }
}

sub parse_raw_logs
{
   my @logs = @_;
   my $valid_stmt;
   my $use_db;

   $/ = ";\n";

   for(@logs)
   {
      open LOG, "< $_" or warn "Could not open raw log '$_': $!\n" and next;
      print "Reading raw log '$_'.\n";

      $use_db = 0;

      while(<LOG>)
      {
         s/^\n//;   # Remove leading \n
         chomp;     # Remove trailing \n
         $_ .= ';'; # Put ; back

         d("parse_raw_logs: read stmt '$_'"); # D

         $valid_stmt = 1;
         /^\s*(\w+)/;
         $q = $1;

         if(lc($q) eq "use")
         {
            /use (\w+)/i;
            $use_db = $1;

            push @q_a, "USE $1;" if ($op{ta} || $op{pq});
            next;
         }
         else
         {
            if(! (exists $filter{uc $1} && !$filter{uc $1}) ) { $valid_stmt = 0; }
            elsif($op{'only-databases'} && !exists $isolate{databases}->{$use_db}) { $valid_stmt = 0; }
            elsif($op{grep} && (! /$op{grep}/io))       { $valid_stmt = 0; }
         }

         if($valid_stmt)
         {
            $stmt = $_;

            if($op{ta} || $op{pq}) { push @q_a, $stmt; }

            if(!$op{nr})
            {
               abstract_stmt(); # Sets $q to abstracted form of $stmt

               my $x = $q_h{$q} ||= { };

               if($need_examples)
               {
                  $x->{sample} = $stmt;
               }

               $x->{c} += 1;
               $x->{db} = $use_db if $x->{c} == 1;

               d("parse_raw_logs: c = $x->{c}, db = $x->{db}, SAVED stmt '$stmt'"); # D
            }
         }
         else { d("parse_raw_logs: INVALID stmt (fails filter, only-dbs, or grep)"); } # D
      }
      close LOG;
   }
}

sub abstract_stmt
{
   $q = lc $stmt;

   # --- Regex copied from mysqldumpslow
   $q =~ s/\b\d+\b/N/g;
   $q =~ s/\b0x[0-9A-Fa-f]+\b/N/g;
   $q =~ s/''/'S'/g;
   $q =~ s/""/"S"/g;
   $q =~ s/(\\')//g;
   $q =~ s/(\\")//g;
   $q =~ s/'[^']+'/'S'/g;
   $q =~ s/"[^"]+"/"S"/g;
   # ---

   $q =~ s/^\s+//g;
   $q =~ s/\s{2,}/ /g;
   $q =~ s/\n/ /g;
   $q =~ s/; (\w+) /;\n$1 /g; # \n between compound statements

   # TODO : need to fix problems w/ IN normalization in subselects 
   # TODO : condense bulk INSERTs into somthing like VALUES (N),(N) --> VALUES (N)2

   while($q =~ /( IN\s*\((?![NS]{1}\d+)(.+?)\))/i)
   {
      my $in = $2;
      my $N = ($in =~ tr/N//);

      if($N)
      {
         $q =~ s/ IN\s*\((?!N\d+)(.+?)\)/ IN (N$N)/i;    # IN (N, N) --> IN (N2)
      }
      else
      {
         $N = ($in =~ tr/S//);
         $q =~ s/ IN\s*\((?!S\d+)(.+?)\)/ IN (S$N)/i;    # IN ('S', 'S') --> IN (S2)
      }
   }
}

sub print_beautifully
{
   # TODO : uh... rewrite this whole thing; the sub itself is not beautiful

   s!^(\w+) !uc "$1 "!eg;
   s!\n(\w+) !uc "\n$1 "!eg;
   s! (from|join|where|order by|limit|as|having|like|null|exists|union) !uc " $1 "!eg;
   s! (select|inner|cross|outer|on|using|between|is|if) !uc " $1 "!eg;
   s! (into|set|left|right|not|table) !uc " $1 "!eg;
   s! (values|natural|and|or|option) !uc " $1 "!eg;
   s! (asc[,;]*|desc[,;]*) !uc " $1 "!eg;
   s! (low_priority|delayed|high_priority|straight_join|sql_no_cache|sql_cache) !uc " $1 "!eg;
   s! \(select !\(SELECT !g;
   s! values\(! VALUES\(!g;
   s! (count\(|min\(|max\(|sum\(|now\()!uc " $1"!eg;
   s! (status|master|slave)\b!uc " $1"!eg;

   print;
}


sub do_email_report {
    my $resultsTableRef;
    my $row;
    my $jobId;
    my $profilerSQL;
    my $emailText = '';
    my $detailsTableRef;
    my $detailsRow;
    my $i = 0;
    my %emailHeaders;
    my $mailer;
    my %orderBy = (
        # Sort by count
        'c'         => 'count',
        # Sort by total query time
        't'         => 'time_total',
        # Sort by average query time
        'at'        => 'time_average',
        # Sort by total lock time
        'l'         => 'lock_time_total',
        # Sort by average lock time
        'al'        => 'lock_time_average',
        # Sort by average rows sent
        'rs'        => 'rows_sent_avg',
        # Sort by max rows sent
        'rs_max'    => 'rows_sent_max',
        # Sort by average rows examined
        're'        => 'rows_examined_avg',
        # Sort by max rows examined
        're_max'    => 'rows_examined_max',
    );
    
    $profilerDbh->do("USE $profilerDatabase");
        
    $profilerQuery = $profilerDbh->prepare("SELECT * FROM sqlprofiler_job WHERE UNIX_TIMESTAMP(processed) > " . (time() - timespec_to_seconds($op{last})) . " AND UNIX_TIMESTAMP(processed) <= " . time());
    $profilerQuery->execute();
    
    $resultsTableRef = $profilerQuery->fetchall_arrayref({});
    while ($row = (shift @$resultsTableRef)) {
        $jobId = $$row{'id'};

        $emailText .= 'Job #' . $jobId . ': ' . ($$row{'log_type'} eq $logTypes{UNKNOWN} ? 'unknown' : $logTypeNames[$$row{'log_type'}]) . ' log named ' . $$row{'filename'} . "\n";
        $emailText .= 'Log begins ' . $$row{'start'} . ' ends ' . $$row{'end'} . ' processed ' . $$row{'processed'} . ($$row{'status'} eq 0 ? ' without error' : ' with errors') . "\n";
        $emailText .= '-' x 100;
        $emailText .= "\n";
        
        $profilerSQL = "SELECT q.*,m.query FROM sqlprofiler_master m, sqlprofiler_queries q, sqlprofiler_job j WHERE j.id=$jobId AND m.sql_hash=q.sql_hash AND q.job_id=j.id";
        $profilerSQL .= " ORDER BY q." . $orderBy{$op{sort}} . " DESC";
        
        $profilerSQL .= " LIMIT $op{top}";

        $profilerQuery = $profilerDbh->prepare($profilerSQL);
        $profilerQuery->execute();

        $detailsTableRef = $profilerQuery->fetchall_arrayref({});
        while ($detailsRow = (shift @$detailsTableRef)) {
            $i++;
            $emailText .= '#' . $i . ': ' . $$detailsRow{'query'} . "\n";
            $emailText .= "\tcount                      : " . $$detailsRow{'count'} . " (" . $$detailsRow{'percent'} . "%)\n";
            if ($$row{'log_type'} eq $logTypes{SLOW}) {
                $$detailsRow{'time_total'}        = defined $$detailsRow{'time_total'} ? $$detailsRow{'time_total'} : 0e0;
                $$detailsRow{'time_min'}          = defined $$detailsRow{'time_min'} ? $$detailsRow{'time_min'} : 0e0;
                $$detailsRow{'time_avg'}          = defined $$detailsRow{'time_avg'} ? $$detailsRow{'time_avg'} : 0e0;
                $$detailsRow{'time_max'}          = defined $$detailsRow{'time_max'} ? $$detailsRow{'time_max'} : 0e0;
                $$detailsRow{'lock_time_total'}   = defined $$detailsRow{'lock_time_total'} ? $$detailsRow{'lock_time_total'} : 0e0;
                $$detailsRow{'lock_time_min'}     = defined $$detailsRow{'lock_time_min'} ? $$detailsRow{'lock_time_min'} : 0e0;
                $$detailsRow{'lock_time_avg'}     = defined $$detailsRow{'lock_time_avg'} ? $$detailsRow{'lock_time_avg'} : 0e0;
                $$detailsRow{'lock_time_max'}     = defined $$detailsRow{'lock_time_max'} ? $$detailsRow{'lock_time_max'} : 0e0;
                $$detailsRow{'rows_sent_min'}     = defined $$detailsRow{'rows_sent_min'} ? $$detailsRow{'rows_sent_min'} : 0e0;
                $$detailsRow{'rows_sent_avg'}     = defined $$detailsRow{'rows_sent_avg'} ? $$detailsRow{'rows_sent_avg'} : 0e0;
                $$detailsRow{'rows_sent_max'}     = defined $$detailsRow{'rows_sent_max'} ? $$detailsRow{'rows_sent_max'} : 0e0;
                $$detailsRow{'rows_examined_min'} = defined $$detailsRow{'rows_examined_min'} ? $$detailsRow{'rows_examined_min'} : 0e0;
                $$detailsRow{'rows_examined_avg'} = defined $$detailsRow{'rows_examined_avg'} ? $$detailsRow{'rows_examined_avg'} : 0e0;
                $$detailsRow{'rows_examined_max'} = defined $$detailsRow{'rows_examined_max'} ? $$detailsRow{'rows_examined_max'} : 0e0;

                $emailText .= sprintf ("\ttime      (tot/min/avg/max): %d/%d/%d/%d\n", $$detailsRow{'time_total'}, $$detailsRow{'time_min'}, $$detailsRow{'time_avg'}, $$detailsRow{'time_max'});
                $emailText .= sprintf ("\tlock time (tot/min/avg/max): %d/%d/%d/%d\n", $$detailsRow{'lock_time_total'}, $$detailsRow{'lock_time_min'}, $$detailsRow{'lock_time_avg'}, $$detailsRow{'lock_time_max'});
                $emailText .= sprintf ("\trows sent     (min/avg/max): %d/%d/%d\n", $$detailsRow{'rows_sent_min'}, $$detailsRow{'rows_sent_avg'}, $$detailsRow{'rows_sent_max'});
                $emailText .= sprintf ("\trows examined (min/avg/max): %d/%d/%d\n", $$detailsRow{'rows_examined_min'}, $$detailsRow{'rows_examined_avg'}, $$detailsRow{'rows_examined_max'});
            }
            $emailText .= "\n";
        }
        
        $i = 0;
        $emailText .= "\n";
    }
    
    $emailHeaders{'To'} = $op{email};
    $emailHeaders{'From'} = $fromAddress;
    $emailHeaders{'Subject'} = 'SQL Profiler Report';
    $mailer = Mail::Mailer->new('sendmail');
    $mailer->open(\%emailHeaders);
    print $mailer $emailText;
    $mailer->close();
    
    exit;
}

sub do_table_insert {
    my $i = $op{top};
    my $x;
    my $jobId;
    my $logType;
    my $logName;
    
    for (keys %q_h) {
        # If the --time-each-query option was used, or user requested
        # sorting by execution time (e) or approx. total query execution
        # time (c*e), run through the queries in %q_h, determining their 
        # times.
        if($op{te} || $op{sort} eq 'e' || $op{sort} eq 'ce') {
            time_query($_);
        }

        # If the user requested sorting by approx. total query execution
        # time, calculate total time by multiplying how many times each given
        # query was seen by its execution time.
        if($op{sort} eq 'ce') {
            $q_h{$_}->{ce} = $q_h{$_}->{c} * $q_h{$_}->{e};
        }

        # If --explain was requested, or if the user requested sorting by
        # rows produced (rp) or rows read (rr), then EXPLAIN each query.
        if($op{ex} || $op{sort} eq 'rp' || $op{sort} eq 'rr') {
            EXPLAIN($_);
        }

        # If we're parsing a slow log, calculate the average query time (at),
        # setting $res as a side effect (resolution, e.g. ms for milliseconds),
        # average lock time (al) and average lock time resolution.  Do this for
        # each query.
        if ($op{slow}) {
            $q_h{$_}->{at} = mnp($q_h{$_}->{t} / $q_h{$_}->{c});
            $q_h{$_}->{at_res} = $res;

            $q_h{$_}->{al} = mnp($q_h{$_}->{l} / $q_h{$_}->{c});
            $q_h{$_}->{al_res} = $res;
        }
    }

    if ($op{general}) {
        $logType = $logTypes{GENERAL};
        $logName = $op{general};
    } elsif ($op{slow}) {
        $logType = $logTypes{SLOW};
        $logName = $op{slow};
    } elsif ($op{raw}) {
        $logType = $logTypes{RAW};
        $logName = $op{raw};
    } else {
        $logType = $logTypes{UNKNOWN};
        $logName = 'Unknown';
    }
    
    # Select the datbase that contains our tables
    $profilerQuery = $profilerDbh->prepare("USE $profilerDatabase");
    $profilerQuery->execute();
    
    if (!defined($logStart)) { $logStart = 0; }
    if (!defined($logEnd)) { $logEnd = 0; }
    
    $profilerQuery = $profilerDbh->prepare("INSERT INTO sqlprofiler_job (server_name,log_type,filename,start,end) "
                        .  "VALUES ('" 
                        .  (defined($dbName) && $dbName ne '' ? $dbName : (defined($dbHost) && $dbHost ne '' ? $dbHost : 'Unknown')) 
                        . "', $logType, '$logName',FROM_UNIXTIME($logStart),FROM_UNIXTIME($logEnd))");
    $profilerQuery->execute();
    $jobId = $profilerQuery->{mysql_insertid};
    
    for(sort { $q_h{$b}->{$op{sort}} <=> $q_h{$a}->{$op{sort}} } keys(%q_h)) {
        my $quotedSQL;
        my @result;
        
        $x = $q_h{$_};
        
        $quotedSQL = $_;
        $quotedSQL =~ s/'/\\'/g;
        $profilerQuery = $profilerDbh->prepare("SELECT count(*) FROM sqlprofiler_master WHERE sql_hash = MD5('$quotedSQL')");
        $profilerQuery->execute();

        my $explainStr = '';
        
        # User wants queries to be EXPLAINed
        if($op{ex}) {
            if($x->{EXPLAIN_err}) {
                $explainStr = "EXPLAIN error: $x->{EXPLAIN_err}";
            } else {
                my $j;

                for($j = 0; $j < (scalar @{$x->{EXPLAIN}}); $j += 2)
                {
                    $explainStr .= $x->{EXPLAIN}[$j] . ": " .  $x->{EXPLAIN}[$j + 1] . "\n";
                }
            }
        }
        

        @result = $profilerQuery->fetchrow_array();
        if (!@result || $result[0] == 0) {
            $profilerQuery = $profilerDbh->prepare("INSERT INTO sqlprofiler_master (sql_hash,introduced,query,query_explain) VALUES (MD5('$quotedSQL'),NOW(),'$quotedSQL','$explainStr')");
            $profilerQuery->execute();
        }

        $profilerQuery = $profilerDbh->prepare("UPDATE sqlprofiler_master SET last_seen=NOW() WHERE sql_hash=MD5('$quotedSQL')");
        $profilerQuery->execute();
        
        
        $profilerQuery = $profilerDbh->prepare("INSERT INTO sqlprofiler_queries (job_id,sql_hash,count,percent,db_name,user) VALUES (" . 
            $jobId . "," .
            "MD5('$quotedSQL')," . 
            $x->{c} . "," . 
            perc($x->{c}, $total_queries) . "," .
            "'" . ($x->{db} ? $x->{db} : 'Unknown') . "'," .
            "'" . ($x->{user} ? $x->{user} : 'Unknown') . "')"
        );
        $profilerQuery->execute();
        
        # The values for sqlprofiler_queries table are only calculated
        # for slow logs.
        if ($op{slow}) {
            my %nth_vals;
            calculate_nth_vals($x->{t_a}, \%nth_vals);
            
            my $sql;
            
            $sql = "UPDATE sqlprofiler_queries SET " .
                "time_total="       . $x->{t} . "," . 
                "time_average="     . $x->{at} . "," . 
                "time_min="         . $x->{t_min} . "," .
                "time_max="         . $x->{t_max} . "," .
                "nthp="             . $op{nthp} . "," .
                "nthp_time_total="  . $nth_vals{sum} . "," .
                "nthp_time_average=". $nth_vals{avg} . "," .
                "nthp_time_min="    . $nth_vals{min} . "," .
                "nthp_time_max="    . $nth_vals{max} . "," .
                "lock_time_total="  . $x->{l} . "," .
                "lock_time_average=". $x->{al} . "," .
                "lock_time_min="    . $x->{l_min} . "," .
                "lock_time_max="    . $x->{l_max} . "," .
                "rows_sent_avg="    . $x->{rs} / $x->{c} . "," .
                "rows_sent_min="    . $x->{rs_min} . "," .
                "rows_sent_max="    . $x->{rs_max} . "," .
                "rows_examined_avg=". $x->{re} / $x->{c} . "," .
                "rows_examined_min=". $x->{rs_min} . "," .
                "rows_examined_max=". $x->{rs_max} .
                " WHERE job_id=$jobId AND sql_hash=MD5('$quotedSQL')";
                
            $profilerQuery = $profilerDbh->prepare($sql);
            $profilerQuery->execute();
            if ($profilerQuery->err()) {
                print "DBI error: " . $profilerQuery->errstr() . "\n";
            }
        }
        
        $i--;
        
        if ($i eq 0) {
            # Exit the for loop after the specified number of queries
            # has been processed.
            last;
        }
    }
    
    $profilerQuery = $profilerDbh->prepare("UPDATE sqlprofiler_job SET processed=NOW() WHERE id=$jobId");
    $profilerQuery->execute();
    
}
sub do_reports
{
   my $i = $op{top};
   my $x;

   for (keys %q_h) {
       # If the --time-each-query option was used, or user requested
       # sorting by execution time (e) or approx. total query execution
       # time (c*e), run through the queries in %q_h, determining their 
       # times.
       if($op{te} || $op{sort} eq 'e' || $op{sort} eq 'ce') {
           time_query($_);
       }
       
       # If the user requested sorting by approx. total query execution
       # time, calculate total time by multiplying how many times each given
       # query was seen by its execution time.
       if($op{sort} eq 'ce') {
           $q_h{$_}->{ce} = $q_h{$_}->{c} * $q_h{$_}->{e};
       }

       # If --explain was requested, or if the user requested sorting by
       # rows produced (rp) or rows read (rr), then EXPLAIN each query.
       if($op{ex} || $op{sort} eq 'rp' || $op{sort} eq 'rr') {
           EXPLAIN($_);
       }
       
       # If we're parsing a slow log, calculate the average query time (at),
       # setting $res as a side effect (resolution, e.g. ms for milliseconds),
       # average lock time (al) and average lock time resolution.  Do this for
       # each query.
       if ($op{slow}) {
           $q_h{$_}->{at} = mnp($q_h{$_}->{t} / $q_h{$_}->{c});
           $q_h{$_}->{at_res} = $res;

           $q_h{$_}->{al} = mnp($q_h{$_}->{l} / $q_h{$_}->{c});
           $q_h{$_}->{al_res} = $res;
       }
   }

   # Crank through all the queries, sorting as user requested
   for(sort { $q_h{$b}->{$op{sort}} <=> $q_h{$a}->{$op{sort}} } keys(%q_h))
   {
      print_report_marker($op{top} - $i + 1);

      # Set x to make dealing with this particular query less cumbersome.
      $x = $q_h{$_};

      printf "Count         : %d (%d%%)\n", $x->{c}, perc($x->{c}, $total_queries);

      # If we're parsing a slow log:
      if($op{slow})
      {
         # Make times "pretty".
         $x->{t}= mnp($x->{t});
         $x->{t_res} = $res;

         $x->{t_min} = mnp($x->{t_min});
         $x->{t_min_res} = $res;
         
         $x->{t_max} = mnp($x->{t_max});
         $x->{t_max_res} = $res;
         
         $x->{l}= mnp($x->{l});
         $x->{l_res}= $res;
         
         $x->{l_min} = mnp($x->{l_min});
         $x->{l_min_res} = $res;
         
         $x->{l_max} = mnp($x->{l_max});
         $x->{l_max_res} = $res;

         # Display timings for this query
         print "Time          : $x->{t} $x->{t_res} total, $x->{at} $x->{at_res} avg, $x->{t_min} $x->{t_min_res} to $x->{t_max} $x->{t_max_res} max\n";

         # Display percentile (default 95th) timings for this query
         print_nth_vals($x->{t_a});
         
         # Display distribution
         print_dist($x->{t_a}) if $op{pd};

         printf "Lock Time     : $x->{l} $x->{l_res} total, $x->{al} $x->{al_res} avg, $x->{l_min} $x->{l_min_res} to $x->{l_max} $x->{l_max_res} max\n";

         printf "Rows sent     : %d avg, %d to %d max\n",
            $x->{rs} / $x->{c}, $x->{rs_min}, $x->{rs_max};

         printf "Rows examined : %d avg, %d to %d max\n",
            $x->{re} / $x->{c}, $x->{re_min}, $x->{re_max};

         printf "User          : %s (%d%%)\n",
            $x->{user}, perc($slow_users{$x->{user}}, $total_queries);
      }

      print "Database      : " . ($x->{db} ? $x->{db} : 'Unknown') . "\n";

      if($op{te} || $op{sort} eq 'e' || $op{sort} eq 'ce')
      {
         $x->{e} = mnp($x->{e}, 1);
         print "Execution time: $x->{e} $res\n";
      }

      if($op{sort} eq 'ce')
      {
         $x->{ce} = mnp($x->{ce}, 1);
         printf "Count * Exec  : $x->{ce} $res";

         if($op{ta}) { printf "(%d%%)\n", perc($x->{ce}, $op{ta}); }
         else { print "\n"; }
      }

      if($op{ex} || $op{sort} eq 'rp' || $op{sort} eq 'rr')
      {
         print "Rows (EXPLAIN): ";

         if(!$x->{EXPLAIN_err}) { print "$x->{rp} produced, $x->{rr} read\n"; }
         else { print "EXPLAIN error: $x->{EXPLAIN_err}\n"; }
      }

      if($op{ex})
      {
         print "EXPLAIN       : ";

         if($x->{EXPLAIN_err})
         {
            print "EXPLAIN error: $x->{EXPLAIN_err}\n";
         }
         else
         {
            my $j;

            print "\n";

            for($j = 0; $j < (scalar @{$x->{EXPLAIN}}); $j += 2)
            {
               print "\t$x->{EXPLAIN}[$j]: $x->{EXPLAIN}[$j + 1]\n";
               if($x->{EXPLAIN}[$j] eq "Extra") { print "\n"; }
            }
         }
      }

      print "\n";

      if($op{examples}) { print $x->{sample}; }
      else
      {
         if(!$op{flat}) { print_beautifully(); } # Beautifies and prints $_
         else { print $_; }
      }

      print "\n";

      last if !--$i;
   }
}

sub timespec_to_seconds {
    my $ts = shift @_;
    my @pieces;
    my %tsHash;
    
    @pieces = split(/([yMwdhms])/, $ts);
    %tsHash = (reverse @pieces);
    
    return (
        (exists $tsHash{'y'} ? $tsHash{'y'} * 31536000  : 0) +
        (exists $tsHash{'M'} ? $tsHash{'M'} * 2592000   : 0) +
        (exists $tsHash{'w'} ? $tsHash{'w'} * 604800    : 0) +
        (exists $tsHash{'d'} ? $tsHash{'d'} * 86400     : 0) +
        (exists $tsHash{'h'} ? $tsHash{'h'} * 3600      : 0) +
        (exists $tsHash{'m'} ? $tsHash{'m'} * 60        : 0) +
        (exists $tsHash{'s'} ? $tsHash{'s'}             : 0)
    );
}

# Process a DSN string as passed by a user.
# Parsed DSN will be made into a hash, which will
# then be returned.
sub parse_dsn {
    my $dsnString = shift @_;
    my $component;
    my ($name,$user,$pass,$host,$db,$port,$socket) = (undef,undef,undef,undef,undef,undef,undef);
    
    for $component (split(/,/, $dsnString)) {
        if ($component =~ /N=(\w[\w\s]+)/) {
            $name = $1;
        } elsif ($component =~ /u=(\w+)/) {
            $user = $1;
        } elsif ($component =~ /p=(.*)/) {
            $pass = $1;
        } elsif ($component =~ /h=(\w[\w\s\.]+)/) {
            $host = $1;
        } elsif ($component =~ /D=(\w[\w\s]+)/) {
            $db = $1;
        } elsif ($component =~ /P=(\d+)/) {
            $port = $1;
        } elsif ($component =~ /S=(\w[\w\s\/]+)/) {
            $socket = $1;
        }
    }
    
    return ($name,$user,$pass,$host,$db,$port,$socket);
}

sub avg
{
   my $avg = shift;
   my @x = @_;
   my $sum = 0;

   $avg = scalar @x if $avg == 0;
   for(@x) { $sum += $_; }
   return sprintf "%.3f", $sum / $avg;
}

sub perc
{
   my($is, $of) = @_;
   return sprintf "%d", ($is * 100) / ($of ||= 1);
}

sub print_dist
{
   my $x = shift; # Ref to array of values
   my %y;
   my $z;
   my $t;
   my $n;

   $z = scalar @$x;
   for(@$x) { $y{$_} ||= 0; $y{$_} += 1; } # Count occurances of each unique value
   for(keys %y) { $y{$_} = perc($y{$_}, $z); } # Save percentage of each unqiue value

   print "Dist. of Time :\n";
   for( sort { $y{$b} <=> $y{$a} } keys %y ) # Sort desc by percentage of each unqiue value
   {
      $t += $y{$_};
      last if ++$n > $op{np};   # Stop if printed number of percentages > max allowed
      last if $y{$_} < $op{mp}; # Stop if percentage < minimum percentage
      print "\t\t$y{$_}\%: $_\n";
   }
   print "\t\t$t\% of total\n"; # Total percentage all the printed percentages account for
}

sub print_nth_vals
{
   my $x = shift; # Ref to array of values
   my @s;
   my $n;
   my $avg;
   my $avg_res;
   my $min;
   my $min_res;
   my $max;
   my $max_res;
   my $sum;
   my $sum_res;

   return if scalar @$x == 1;

   @s = sort { $a <=> $b } @$x;
   $n = ((scalar @$x) * $op{nthp}) / 100;
   @s = splice(@s, 0, $n);

   $avg = mnp(avg(0, @s));
   $avg_res = $res;

   $min = mnp($s[0]);
   $min_res = $res;

   $max = mnp($s[$n - 1]);
   $max_res = $res;

   for(@s) { $sum += $_ };
   $sum = mnp($sum);
   $sum_res = $res;

   print "$op{nthp}\% of Time   : $sum $sum_res total, $avg $avg_res avg, $min $min_res to $max $max_res max\n";
}

sub calculate_nth_vals
{
   my $x = shift; # Ref to array of values
   my $nth_vals = shift;
   my @s;
   my $n;
   my $sum;

   return if scalar @$x == 1;

   @s = sort { $a <=> $b } @$x;
   $n = ((scalar @$x) * $op{nthp}) / 100;
   @s = splice(@s, 0, $n);

   $$nth_vals{avg} = avg(0, @s);
   $$nth_vals{min} = $s[0];
   $$nth_vals{max} = $s[$n - 1];
   
   for(@s) { $sum += $_ };
   $$nth_vals{sum} = $sum;
}


sub print_queries
{
   print "\n__ All Queries _________________________________________________________\n\n";
   for(@q_a) { print "$_\n\n"; }
}

sub EXPLAIN
{
   my $k = shift;
   my $row;
   my @rows;
   my $col;
   my ($i, $j);
   my $x;

   $x = $q_h{$k};
   d("EXPLAIN: k = '$k'");
   d("Dump of \$x:\n" . Dumper($x));

   $x->{EXPLAIN_err} = 0;
   $x->{rp} = -1;
   $x->{rr} = -1;

   if($x->{sample} !~ /^SELECT/i)
   {
      $x->{EXPLAIN_err} = "Not a SELECT statement.";
      return;
   }

   if(!$x->{db})
   {
      if(!$op{db})
      {
         $x->{EXPLAIN_err} = "Unknown database.";
         return;
      }
      else
      {
         for(keys %dbs)
         {
            $dbh->do("USE $_;");
            
            $query = $dbh->prepare("EXPLAIN $x->{sample};");
            $query->execute();
            next if $DBI::err;

            $x->{db} = $_;
            last;
         }

         if(!$x->{db})
         {
            $x->{EXPLAIN_err} = "Unknown database and no given databases work.";
            return;
         }
      }
   }

   d("About to prepare, dump of \$x:\n" . Dumper($x));
   d("Dump of \$dbh:\n" . Dumper($dbh));
   $query = $dbh->prepare("USE $x->{db};");
   $query->execute();
   if($DBI::err)
   {
      $x->{EXPLAIN_err} = $DBI::errstr;
      return;
   }

   $query = $dbh->prepare("EXPLAIN $x->{sample};");
   $query->execute();
   if($DBI::err)
   {
      $x->{EXPLAIN_err} = $DBI::errstr;
      return;
   }

   $x->{EXPLAIN} = [];

   while($row = $query->fetchrow_hashref())
   {
      push @rows, ($row->{rows} ? $row->{rows} : 0);

      for($j = 0; $j < $query->{NUM_OF_FIELDS}; $j++)
      {
         $col = $query->{NAME}->[$j];

         push @{$x->{EXPLAIN}}, $col;
         push @{$x->{EXPLAIN}}, ($row->{$col} ? $row->{$col} : '');
      }
   }

   for($i = 0, $j = 1; $i < $query->rows; $i++) { $j *= $rows[$i]; }
   $x->{rp} = $j; # Rows produced
   $x->{rr} = calc_rows_read(@rows);
}

sub time_all_queries
{
   print "\n__ All Queries Execution Time_________________________________________________\n";
   print "Averaging over $op{avg} runs: ";
   time_profile(1, @q_a);
   $op{ta} = avg($op{avg}, @t);
   print "\nAverage: $op{ta} seconds\n";
}


sub time_query
{
   my $k = shift;
   my @x;

   $x[0] = $q_h{$k}->{sample};
   time_profile(0, @x);
   $q_h{$k}->{e} = avg($op{avg}, @t);
}

sub time_profile
{
   my $print = shift;
   my @q = @_;
   my $n = $op{avg} ||= 1; # Number of time runs
   my $perc = 0;
   my ($i, $j);
   my $r;

   $i = 1;
   $j = '25', $perc = int $n / 4 if $op{percent} || $n >= 20; # Percentage interval
   @t = ();

   while($i++ <= $n)
   {
      if($print) {
         if($perc) {
            if($i == $perc) {
               print "$j\% ";
               $j += 25;
               $perc += $perc;
            }
         }
         else { print $i - 1 . ' '; }
      }

      $t0 = [gettimeofday];
      for(@q)
      {
         $r = $dbh->do($_);
         print "\ntime_profile: '$_'\nMySQL error: $DBI::errstr\n" if (!defined $r && $op{debug});
      }
      $t1 = [gettimeofday];
      $t  = tv_interval($t0, $t1);
      push(@t, $t);
   }
}

sub calc_rows_read
{
   my @rows = @_;

   my ($n_rows, $total);
   my ($i, $j, $x);

   $n_rows = scalar @rows;
   $total  = $rows[0];

   for($i = 1; $i < $n_rows; $i++) {
      for($j = 1, $x = $rows[0]; $j <= $i; $j++) { $x *= $rows[$j]; }
      $total += $x;
   }

   return $total;
}

sub set_filter
{
   my @f = split ',', $op{filter};
   my ($x, $s);

   for(@f)
   {
      ($x, $s) = /(.)(.*)/;
      if($x ne "+" && $x ne "-") { print "Ignoring invalid filter set '$x'.\n" and next; }
      $s = uc $s;
      if($s eq "*") {
         for(keys %filter) {
            $filter{$_} = 1 if $x eq "-";
            $filter{$_} = 0 if $x eq "+";
         }
         next;
      }

      if(not exists $filter{$s}) { print "Ignorning invalid filter '$s'.\n" and next; }
      else {
         $filter{$s} = 1 if $x eq "-";
         $filter{$s} = 0 if $x eq "+";
      }
   }

   print "Allowed SQL statements: ";
   for(keys %filter) { print "$_ " if $filter{$_} == 0; }
   print "\n";

   # TODO : can't filter SELECTs in UNIONs (Bill)
}

sub isolate_x
{
   my $x = shift; # where x is --only-x
   my @y = split ',', $op{"only-$x"};

   print "Only $x: ";

   for(@y)
   {
      $isolate{$x}->{$_} = 0;
      print "$_ ";
   }

   print "\n";
}

sub print_report_marker
{
   my $val = shift;
   printf "\n__ %03d _______________________________________________________________________\n\n", $val;
}

# make number pretty
sub mnp
{
   my $n = shift;
   my $force_milli = shift;

   $force_milli ||= 0;

   if(!$op{milliseconds} && !$force_milli)
   {
      $res = 's';
      return sprintf "%d", $n;
   }

   if($n >= 1)
   {
      $res = 's';
   }
   else
   {
      $res = 'ms';
      $n *= 1000;
   }

   $n = sprintf "%.3f", $n;

   return $n;
}

# debug
sub d
{
   return unless $op{debug};

   my $debug_msg = shift;

   $debug_msg =~ s/\n\'$/'/;

   print "$debug_msg\n";
}
