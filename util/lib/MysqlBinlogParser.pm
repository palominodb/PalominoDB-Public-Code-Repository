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

## This package parses a mysql binlog according to the details found in:
## http://forge.mysql.com/wiki/MySQL_Internals_Binary_Log
## It makes very little effort, currently, to Interpret the details of the various
## events found in the binlog, instead opting to provide a simple stream interface
## to every event and leave it up to the application to interpret the data.
## As an exception, this package does provide constants that correspond to the
## defined event types for easier use.
package MysqlBinlogParser;
use strict;
use warnings FATAL => 'all';
use MIME::Base64;
use Fcntl qw(:seek);
use Carp;

## These constants describe attributes useful for reading a binlog event
use constant {
  MAGIC_LEN => 4,
  MAGIC_BYTES => "\xfe\x62\x69\x6e",
  V1_HEADER_LEN => 13,
  ## V2 is obsolete, and not in the wild
  V3_HEADER_LEN => 19,
  ## V4 len is the same, but may contain additional data, later
  V4_HEADER_LEN => 19,
  ## Length of the server version field.
  SERVER_VERSION_LEN => 50,

  ## Length of the first event in a binlog, depending
  ## on the version of the binlog.
  V1_EVENT_START_LEN => 69,
  V3_EVENT_START_LEN => 75,
  EVENT_FORMAT_DESC_LEN => 91
};

## These constants give names to the flags
## described by the flags field of the event header.
## They are derived from the values in the 5.1.41 release
## of MySQL in sql/log_event.h
use constant {
  LOG_EVENT_BINLOG_IN_USE_F => 0x01,
  LOG_EVENT_THREAD_SPECIFIC_F => 0x04,
  LOG_EVENT_SUPPRESS_USE_F => 0x08,
  LOG_EVENT_UPDATE_TABLE_MAP_VERSION_F => 0x10,
  LOG_EVENT_ARTIFICIAL_F => 0x20,
  LOG_EVENT_RELAY_LOG_F => 0x40
};

## These constants correspond to event types as found in the
## 5.1.41 release of MySQL.
## They were pulled from:
## - sql/log_event.h
use constant {
  EVENT_UNKNOWN=> 0,
  EVENT_START_V3=> 1,
  EVENT_QUERY=> 2,
  EVENT_STOP=> 3,
  EVENT_ROTATE=> 4,
  EVENT_INTVAR=> 5,
  EVENT_LOAD=> 6,
  EVENT_SLAVE=> 7,
  EVENT_CREATE_FILE=> 8,
  EVENT_APPEND_BLOCK=> 9,
  EVENT_EXEC_LOAD=> 10,
  EVENT_DELETE_FILE=> 11,
  # EVENT_NEW_LOAD is like EVENT_LOAD except that it has a longer
  # sql_ex, allowing multibyte TERMINATED BY etc; both types share the
  # same class (Load_log_event)
  EVENT_NEW_LOAD=> 12,
  EVENT_RAND=> 13,
  EVENT_USER_VAR=> 14,
  EVENT_FORMAT_DESCRIPTION=> 15,
  EVENT_XID=> 16,
  EVENT_BEGIN_LOAD_QUERY=> 17,
  EVENT_EXECUTE_LOAD_QUERY=> 18,
  EVENT_TABLE_MAP => 19,
  # These event numbers were used for 5.1.0 to 5.1.15 and are
  # therefore obsolete.
  EVENT_PRE_GA_WRITE_ROWS => 20,
  EVENT_PRE_GA_UPDATE_ROWS => 21,
  EVENT_PRE_GA_DELETE_ROWS => 22,
  # These event numbers are used from 5.1.16 and forward
  EVENT_WRITE_ROWS => 23,
  EVENT_UPDATE_ROWS => 24,
  EVENT_DELETE_ROWS => 25,
  # Something out of the ordinary happened on the master
  EVENT_INCIDENT=> 26,
};

## These constants correspond to the EVENT_QUERY
## status variable types and are used in decoding such events.
## They come from MySQL 5.1.41 in sql/log_event.h.
use constant {
  Q_FLAGS2_CODE => 0,
  Q_SQL_MODE_CODE => 1,
  Q_CATALOG_CODE => 2,
  Q_AUTO_INCREMENT => 3,
  Q_CHARSET_CODE => 4,
  Q_TIME_ZONE_CODE => 5,
  Q_CATALOG_NZ_CODE => 6,
  Q_LC_TIME_NAMES_CODE => 7,
  Q_CHARSET_DATABASE_CODE => 8,
  Q_TABLE_MAP_FOR_UPDATE_CODE => 9
};

## These constants correspond to the available types
## for EVENT_USER_VAR binlog events.
## They come from MySQL 5.1.41 in include/mysql_com.h
use constant {
  U_STRING_RESULT => 0,
  U_REAL_RESULT   => 1,
  U_INT_RESULT    => 2,
  U_ROW_RESULT    => 4,
  U_DECIMAL_RESULT => 5
};

sub new {
  my $class = shift;
  return $class->open(@_);
}

sub open {
  my ($class, $path_or_fh) = @_;
  my $self = {};
  bless $self, $class;
  if(ref($path_or_fh) and ref($path_or_fh) eq 'GLOB') {
    $$self{fh} = $path_or_fh;
    $$self{path} = '';
  }
  else {
    my $tmpfh;
    open($tmpfh, "<", $path_or_fh) or croak($!);
    binmode($tmpfh);
    $$self{fh} = $tmpfh;
    $$self{path} = $path_or_fh;
  }

  # used for handling different binlog version with
  # different sized headers
  $$self{header_length} = 0;
  $$self{log_version} = -1;
  $$self{closed_properly} = -1;
  $$self{created_at} = -1;
  $self->_read_header;
  return $self;
}

# Constructs a new event hashref with standard items from a v1 header
sub _new_event {
  my ($evt_time, $evt_type, $evt_len, $srv_id, $next, $flags) = @_;
  return { ts => $evt_time, type => $evt_type,
           len => $evt_len, server_id => $srv_id,
           next_position => $next, flags => $flags
         };
}

## Reads in the first 13+x bytes, determines the log
## version, whether or not the binlog was closed properly, etc.
sub _read_header {
  my ($self) = @_;
  my $fh = $$self{fh};
  # Used as a temporary buffer
  my ($buf, $evt_type, $srv_id, $evt_len);
  my $header_event;
  CORE::read($fh, $buf, MAGIC_LEN) or croak($!);
  croak("Invalid binlog magic '$buf'") unless($buf eq MAGIC_BYTES);
  CORE::read($fh, $buf, V1_HEADER_LEN) or croak($!);
  ($$self{created_at}, $evt_type, $srv_id, $evt_len) = unpack('LCLL', $buf);
  $header_event = _new_event($$self{created_at}, $evt_type, $evt_len, $srv_id);


  # Now that we've read and upacked the basic information
  # from the initial event, we can set about figuring out what
  # binlog version we're reading. This is necessary because,
  # even though all binlog formats have their version encoded,
  # it's moved around between versions.
  #
  # TODO: We only support v4 binlogs at the moment.
  # Even though we only suport v4 binlogs, we still go about
  # reading the initial event in a compatible way, so as to handle
  # being given older binlogs, and possibly make way for supporting
  # older ones later.
  if($evt_type == EVENT_START_V3 and $evt_len == V1_EVENT_START_LEN) {
    $$self{log_version} = 1;
    croak('Binlogs in v1 format not supported');
  }
  elsif($evt_type == EVENT_START_V3 and $evt_len == V3_EVENT_START_LEN) {
    $$self{log_version} = 3;
    croak('Binlogs in v3 format not supported');
  }
  elsif($evt_type == EVENT_FORMAT_DESCRIPTION) {
    $$self{log_version} = 4;
    $self->_format_description_event($header_event);
    $$self{header_length} = V4_HEADER_LEN;
    $$self{header} = $header_event;

    # Setup the handlers for various event types:
    $$self{handlers} = [];
    $$self{handlers}->[EVENT_QUERY] = \&_v4_query_event;
    $$self{handlers}->[EVENT_XID] = \&_v4_xid_event;
    $$self{handlers}->[EVENT_BEGIN_LOAD_QUERY] = \&_v4_append_block_event;
    $$self{handlers}->[EVENT_EXECUTE_LOAD_QUERY] = \&_v4_execute_load_query_event;
    $$self{handlers}->[EVENT_ROTATE] = \&_v4_rotate_event;
    $$self{handlers}->[EVENT_RAND] = \&_v4_rand_event;
    $$self{handlers}->[EVENT_INTVAR] = \&_v4_intvar_event;
    $$self{handlers}->[EVENT_APPEND_BLOCK] = \&_v4_append_block_event;
    $$self{handlers}->[EVENT_USER_VAR] = \&_v4_user_var_event;
    $$self{handlers}->[EVENT_DELETE_FILE] = \&_delete_file_event;
    $$self{handlers}->[EVENT_WRITE_ROWS] = \&_v4_write_rows_event;
    $$self{handlers}->[EVENT_UPDATE_ROWS] = \&_v4_write_rows_event;
    $$self{handlers}->[EVENT_DELETE_ROWS] = \&_v4_write_rows_event;
    $$self{handlers}->[EVENT_TABLE_MAP] = \&_v4_table_map_event;
  }
  else {
    $$self{log_version} = 3;
    # Some 4.0 and 4.1 binlogs didn't cut a proper START_V3_EVENT at
    # the beginning of every logfile. Only the first one after the master
    # started. This is present to handle that situation.
    croak('Binlogs in v3 format not supported');
  }

}

## seek to a position in the binlog
## presently does no validation of supplied position
sub seek {
  my ($self, $pos) = @_;
  unless(CORE::seek($$self{fh}, SEEK_SET, $pos)) {
    croak($!);
  }
  return 0;
}

## Low-level read routine which just returns an event with its binary payload.
sub read {
  my ($self) = @_;
  my $raw = '';
  my $buf;
  my $evt;
  my ($evt_time, $evt_type, $srv_id, $evt_len, $evt_next, $evt_flags);
  # print(STDERR "# tell: ". tell($$self{fh}) . "\n");
  $_ = CORE::read($$self{fh}, $buf, $$self{header_length});
  if(defined($_) and $_ == 0) { # end of file reached
    return undef;
  }
  elsif(not defined($_)) {
    croak($!);
  }
  $raw .= $buf;
 #print(STDERR "# read bytes: $_\n");

  if($$self{log_version} == 4) {
     ($evt_time, $evt_type, $srv_id, $evt_len, $evt_next, $evt_flags)
       = unpack('LCLLLS', $buf);
     $evt = _new_event($evt_time, $evt_type, $evt_len,
                       $srv_id, $evt_next, $evt_flags);
     # Some events only have header data, no payload,
     # post header data, variable or fixed data.
     if($evt_len - $$self{header_length} == 0) {
       return $evt;
     }

     $_ = CORE::read($$self{fh}, $buf, $evt_len - $$self{header_length});
     if(defined($_) and $_ == 0) {
       return undef;
     }
     elsif(not defined($_)) {
       croak($!);
     }
     $raw .= $buf;
     $$evt{data} = $buf;
     # NOTE: It seems like this might eventually be a performance bottleneck
     # NOTE: but obviously not compared to the IO we're doing.
     eval {
       &{$$self{handlers}->[$$evt{type}]}($evt, $raw);
     };
     if($@ and $@ =~ /Use of uninitialized value in subroutine entry/) {
       croak("No handler for event type $$evt{type}");
     }
     elsif($@) {
       croak($@);
     }
  }
  else {
    croak('Old log format');
  }

  return $evt;
}

# ############################################################################
# Specialized event parsing routines follow
# ############################################################################

sub _format_description_event {
  my ($self, $evt) = @_;
  my $fh = $$self{fh};
  my $buf;
  $$evt{server_version} = '';
  $$evt{create_timestamp} = -1;
  $$evt{header_length} = -1;
  $$evt{event_lengths} = [];

  # read in the remainder of the v4 header
  CORE::read($fh, $buf, V4_HEADER_LEN-(V1_HEADER_LEN)) or croak($!);
  ($$evt{next_position}, $$evt{flags}) = unpack('LS', $buf);

  # read in the data portion of the format description event
  CORE::read($fh, $buf, $$evt{len} - V4_HEADER_LEN) or croak($!);

  # unpack the data portion of the event
  ($$self{log_version}, $$evt{server_version},
   $$evt{create_timestamp}, $$evt{header_length})
    = unpack('Sa['. SERVER_VERSION_LEN .']LC', $buf);
  $$evt{server_version} =~ s/\0//g; # remove the null padding from the server_version field.

  # this unpacks the post headers by skipping everything else previously unpacked
  $$evt{event_lengths} = [unpack('x[S]x['. SERVER_VERSION_LEN .']x[L]xC/C', $buf)];

  # add -1 for EVENT_UNKNOWN and EVENT_START_V3, since it's not included in the
  # post headers length field, which describes the fixed data portion of each event.
  # this is just to make indexing the array easier.
  unshift @{$$evt{event_lengths}}, 0;
  unshift @{$$evt{event_lengths}}, 0;

  # Set the closed_properly flag for this binlog.
  # Confusing: $$evt{flags} should NOT have this set if the binlog was closed properly.
  # We invert this for our sanity.
  $$self{closed_properly} = !($$evt{flags} & LOG_EVENT_BINLOG_IN_USE_F);
}

## Status Variable code common to EVENT_QUERY (2), and EVENT_EXECUTE_LOAD_QUERY (19)
sub _parse_status_variables {
  my ($evt, $stat_vars_len, $stat_vars) = @_;
  for(my $i = 0; $i < $stat_vars_len; $i++) {
    ($_) = unpack("x[$i]C", $stat_vars);
    # print(STDERR "# found variable: $_\n");
    if($_ == Q_FLAGS2_CODE) {
      ($$evt{flags2}) = unpack("x[$i]xL", $stat_vars);
      $i += 4;
    }
    elsif($_ == Q_SQL_MODE_CODE) {
      ($$evt{sql_mode}) = unpack("x[$i]xQ", $stat_vars);
      $i += 8;
    }
    elsif($_ == Q_CATALOG_CODE or $_ == Q_CATALOG_NZ_CODE) {
      my $len;
      ($len, $$evt{catalog_code}) = unpack("x[$i]xCXC/a", $stat_vars);
      $i += $len+1;
    }
    elsif($_ == Q_CHARSET_CODE) {
      ($$evt{character_set_client},
       $$evt{collation_connection},
       $$evt{collation_server}) = unpack("x[$i]xSSS", $stat_vars);
      $i += 6;
    }
    elsif($_ == Q_AUTO_INCREMENT) {
      ($$evt{auto_increment_increment},
       $$evt{auto_increment_offset}) = unpack("x[$i]xSS", $stat_vars);
    }
    elsif($_ == Q_TIME_ZONE_CODE) {
      my $len;
      ($len, $$evt{timezone}) = unpack("x[$i]xCXC/a", $stat_vars);
      $i += $len+1;
    }
    elsif($_ == Q_LC_TIME_NAMES_CODE) {
      ($$evt{lc_time_names}) = unpack("x[$i]xS", $stat_vars);
      $i += 2;
    }
    elsif($_ == Q_CHARSET_DATABASE_CODE) {
      ($$evt{database_charset}) = unpack("x[$i]xS", $stat_vars);
      $i += 2;
    }
    elsif($_ == Q_TABLE_MAP_FOR_UPDATE_CODE) {
      ($$evt{table_map_for_update_bitmap}) = unpack("x[$i]xQ", $stat_vars);
      $i += 8;
    }
    else {
      croak("Unknown status variable $_");
    }
  }
}

## Decodes a MySQL packed integer.
## Returns the number of bytes consumed, and the number.
sub _unpack_int {
  my ($bytes) = @_;
  my $u; # consumed bytes
  ($_) = unpack('C', $bytes);
  if($_ == 252) {
    ($_) = unpack('x[C]S', $bytes);
    $u = 3;
  }
  elsif($_ == 253) {
    ($_) = unpack('L', $bytes);
    $_ &= 0x00ffffff;
    $u = 4;
  }
  elsif($_ == 254) {
    ($_) = unpack('x[C]Q', $bytes);
    $u = 9;
  }
  else {
    $u = 1;
  }
  # print(STDERR "# _unpack_int: u:$u v:$_\n");
  return ($u, $_);
}

## v4 EVENT_QUERY (2) handler
sub _v4_query_event {
  my ($evt) = @_;
  my ($stat_vars_len, $stat_vars, $db_len);
  ($$evt{thread_id}, $$evt{exec_time},
   $db_len, $$evt{error_code}, $stat_vars_len) = unpack('LLCSS', $$evt{data});
  ($stat_vars) = unpack("x[LLCSS]a[$stat_vars_len]", $$evt{data});
  _parse_status_variables($evt, $stat_vars_len, $stat_vars);
  ($$evt{database}, $$evt{stmt}) = unpack("x[LLCSS]x[$stat_vars_len]Z*a*", $$evt{data});
  delete $$evt{data};
}


## v4 EVENT_ROTATE (4)
sub _v4_rotate_event {
  my ($evt) = @_;
  ($$evt{rotate_pos}) = unpack('Q', $$evt{data});
  ($$evt{rotate_file}) = unpack('x[Q]a*', $$evt{data});
  delete $$evt{data};
}

## v4 EVENT_INTVAR (5)
sub _v4_intvar_event {
  my ($evt) = @_;
  ($$evt{intvar_type}, $$evt{intvar_value}) = unpack('CQ', $$evt{data});
  delete $$evt{data};
}

## v4 EVENT_APPEND_BLOCK (9)
sub _v4_append_block_event {
  my ($evt) = @_;
  ($$evt{file_id}, $$evt{file_data}) = unpack('La*', $$evt{data});
  delete $$evt{data};
}

## v3/v4 EVENT_DELETE_FILE (11)
sub _delete_file_event {
  my ($evt) = @_;
  ($$evt{file_id}) = unpack('L', $$evt{data});
  delete $$evt{data};
}

## v4 EVENT_RAND (13)
sub _v4_rand_event {
  my ($evt) = @_;
  ($$evt{rand_seed1}, $$evt{rand_seed2}) = unpack('QQ', $$evt{data});
  delete $$evt{data};
}

## v4 EVENT_USER_VAR (14)
sub _v4_user_var_event {
  my ($evt) = @_;
  ($$evt{variable_name}, $$evt{variable_null},
   $$evt{variable_type}, $$evt{variable_character_set},
   $$evt{variable_length}, $$evt{variable_value}) = unpack('L/aCCLLa*', $$evt{data});
  use Data::Dumper;
  # print(STDERR "# _v4_user_var_event: ". Dumper($evt));
  if($$evt{variable_null} == 0) {
    if($$evt{variable_type} == U_INT_RESULT) {
      ($$evt{variable_value}) = unpack('Q', $$evt{variable_value});
    }
    elsif($$evt{variable_type} == U_REAL_RESULT) {
      ($$evt{variable_value}) = unpack('f', $$evt{variable_value});
    }
    elsif($$evt{variable_type} == U_DECIMAL_RESULT) {
      ($$evt{variable_value}) = unpack('d', $$evt{variable_value});
    }
  }
  delete $$evt{data};
}

## v4 EVENT_XID (16) handler
sub _v4_xid_event {
  my ($evt) = @_;
  ($$evt{xid}) = unpack('Q', $$evt{data});
  delete $$evt{data};
}

## v4 EVENT_BEGIN_LOAD_QUERY (17)
# This event and APPEND_BLOCK_EVENT (9) have the exact same format.
# No need to duplicate code.
# sub _v4_begin_load_query_event {
#  my ($evt) = @_;
#}

## v4 EVENT_EXECUTE_LOAD_QUERY (18)
sub _v4_execute_load_query_event {
  my ($evt) = @_;
  my ($stat_vars_len, $stat_vars, $db_len);
  ($$evt{thread_id}, $$evt{exec_time},
   $db_len, $$evt{error_code}, $stat_vars_len,
   $$evt{file_id}, $$evt{file_name_start},
   $$evt{file_name_end}, $$evt{dup_handling}
  ) = unpack('LLCSSLLLC', $$evt{data});
  ($stat_vars) = unpack("x[LLCSSLLLC]a[$stat_vars_len]", $$evt{data});
  _parse_status_variables($evt, $stat_vars_len, $stat_vars);
 ($$evt{database}, $$evt{stmt}) = unpack("x[LLCSSLLLC]x[$stat_vars_len]Z*a*", $$evt{data});
  delete $$evt{data};
}

sub _hex_dump {
  my ($mem) = @_;
  my $i = 0;
  map( ++$i % 16 ? $_." " : $_ ."\n",
             unpack( 'H2' x length( $mem ), $mem ) ),
               length( $mem ) % 16 ? "\n" : '';
}

## v4 EVENT_TABLE_MAP (19)
sub _v4_table_map_event {
  my ($evt, $raw) = @_;
  # The table_id field in the table map event is a 6-byte integer.
  # This makes unpacking it just a tad annoying.
  my ($s1, $s2, $s3, $i);
  ($s1, $s2, $s3, $$evt{reserved_flags}, $$evt{database}, $$evt{table}, $i)
    = unpack('SSSSxZ*xZ*a[9]', $$evt{data});
  # print(STDERR "# _v4_table_map_event: table_id:", _hex_dump(pack('Q', $s1 + (($s2 << 16) + ($s3 << 32)))) , "\n");
  $$evt{table_id} =  $s1 + (($s2 << 16) + ($s3 << 32));
  ($_, $i) = _unpack_int($i);
  # tbl_id_len(8) + flags_len(2) + database_len(1) + null(1)
  # + table_len(1) + null(1) = 12
  $s1 = $_ + length($$evt{database}) + length($$evt{table})+12;
  # print(STDERR "# _v4_table_map_event: parsed_len: $s1 event_len: $$evt{len}\n");
  $$evt{column_types} = [unpack("x[$s1]C[$i]", $$evt{data})];
  $s1 += $i;
  # print(STDERR "# _v4_table_map_event: parsed_len: $s1 event_len: $$evt{len}\n");
  ($i) = unpack("x[$s1]a[9]", $$evt{data});
  ($_, $i) = _unpack_int($i);

# NOTE: The metadata block and columns_nullable bitmap
# NOTE: are not unpacked, because there's only a small
# NOTE: chance that they'll be needed for inspection.
#  $s1 += $_;
#  ($$evt{metadata_block}) = unpack("x[$s1]a[$i]", $$evt{data});
#  $s1 += $i;
#  ($$evt{columns_nullable}) = unpack("x[$s1]a*", $$evt{data});
  $$evt{data} = encode_base64($raw);
}

## v4 EVENT_WRITE_ROWS (23)
sub _v4_write_rows_event {
  my ($evt, $raw) = @_;
  $$evt{data} = encode_base64($raw);
}

1;
