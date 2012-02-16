require 'rubygems'
require 'sqlite3'
require 'net/ssh'
require 'net/sftp'
require 'digest/sha2'
require 'time'
require 'fileutils'
require 'ifo_get/plugin'


class SarLogs < IfoGet::Plugin
  def initialize(global_keys)
    super
    @dbh = SQLite3::Database.new( global_keys.sar_db )
    begin
      @local_files = @dbh.execute('SELECT hash FROM "loaded_file"')
      @local_files.map! { |r| r[0] }
    rescue SQLite3::SQLException
      @local_files = []
    end

  end
  def finalize()
    @dbh.close
    SarConvert.new(@global_keys.sar_db, @global_keys.sar_basedir).run()
  end
  def self.host_keys()
    return [ 'sarlogs' ]
  end
  def self.group_keys()
    return [ 'sarlogs' ]
  end
  def self.global_keys()
    return [ 'sar_db', 'sar_basedir' ]
  end

  def process(ssh, group, host, group_keys, host_keys)
    return unless group_keys.sarlogs or host_keys.sarlogs
    dl_path = "#{@global_keys.sar_basedir}/#{group}/#{host}/"
    begin
      File.stat(dl_path)
    rescue Errno::ENOENT
      FileUtils.mkdir_p dl_path
    end
    csums = ssh.exec!("openssl sha1 /var/log/sa/*")
    if csums =~ /No such file/
      return false
    end
    sftp = ssh.sftp
    csums.split("\n").each do |l|
      path, sha1 = l.split("= ")
      if path =~ /SHA1\((.*?)\)$/
        path = $1
      end
      if path =~ /sar\d+/ or @local_files.include? sha1
        next
      else
        sftp.download(path, dl_path + File.basename(path),
          :progress => FileDownloadHandler.new('SarLogs', host_keys, true))
      end
    end
    begin
      sftp.loop
    rescue RuntimeError => e
      $stderr.puts "SarLogs ERROR: #{e}"
      retry
    end
    return true
  end
end

class SarConvert
  def initialize(dbfile, basedir)
    @dbfile = dbfile
    @basedir = basedir
    if @basedir !~ /\/$/
      @basedir += '/'
    end
    @dbh = SQLite3::Database.new( @dbfile )
  end

  def run()
    make_datafile()
    prepare_statements()

    @dbh.transaction
    load_datadir()
    @dbh.commit

    close_statements()
  end

  def load_datadir(dir=@basedir)
    if ! File.directory?(dir)
      puts "#{dir} is not a directory."
      return nil
    end
    #@dbh.execute(%Q{SAVEPOINT '#{dir}'})
    puts "Reading: #{dir}"
    if dir !~ /\/$/
      dir += '/'
    end
    Dir.foreach(dir) do |fname|
      next if fname == '.' or fname == '..'
      if File.directory?(dir + fname)
        load_datadir(dir + fname)
      elsif fname =~ /sa\d+/
        load_saDD(dir + fname)
      end
    end
    #@dbh.execute(%Q{RELEASE '#{dir}'})
  end

  def open_sadf(file, *args)
    return IO.popen("sadf -d #{file} -- #{args.join(' ')}")
  end

  def read_sadf(ioh)
    ary=nil
    begin
      ary=ioh.readline().split(';')
    rescue EOFError
      return nil
    end
    if @cached_hostname == ary[0]
      ary[0] = @cached_host_id
    else
      @cached_hostname = ary[0]
      @cached_host_id = ary[0] = get_host(ary[0])
    end
    return ary
  end

  def close_sadf(ioh)
    return ioh.close
  end

  # host, interval, time, cpuN, %nice, %system, %iowait, %steal, %idle
  def load_cpu(file)
    io=open_sadf(file, '-P', 'ALL')
    while ln=read_sadf(io)
      next if ln[3] == '-1'
      @add_cpu.execute(ln[0], ln[2], ln[3].to_i, ln[4,5].map { |i| i.to_f })
    end
    close_sadf(io)
  end

  def load_paging(file)
    io=open_sadf(file, '-B')
    while ln=read_sadf(io)
      @add_paging.execute(ln[0],ln[2], ln[3,6].map { |i| i.to_f })
    end
    close_sadf(io)
  end

  # host;interval;date;-1;count
  def load_intr(file)
    io=open_sadf(file, '-I', 'SUM')
    while ln=read_sadf(io)
      @add_intr.execute(ln[0],ln[2], ln[4].to_f)
    end
    close_sadf(io)
  end

  def load_proc(file)
    io=open_sadf(file, '-c')
    while ln=read_sadf(io)
      @add_proc.execute(ln[0],ln[2], ln[3].to_f)
    end
    close_sadf(io)
  end

  def load_disk(file)
    io=open_sadf(file, '-b')
    while ln=read_sadf(io)
      @add_disk.execute(ln[0],ln[2], ln[3,5].map { |i| i.to_f })
    end
    close_sadf(io)
  end
  # host;interval;time;iface;rxpck;txpck;rxbyt;txbyt;rxcmp;txcmp;rxmcst
  def load_net(file)
    io=open_sadf(file, '-n', 'DEV')
    while ln=read_sadf(io)
      @add_net.execute(ln[0],ln[2], ln[3], ln[4,6].map { |i| i.to_f })
    end
    close_sadf(io)
  end
  # host;interval;time;totsck;tcpsck;udpsck;rawsck;ip_frag
  def load_sock(file)
    io=open_sadf(file, '-n', 'SOCK')
    while ln=read_sadf(io)
      @add_sock.execute(ln[0], ln[2], ln[3,5].map { |i| i.to_i })
    end
    close_sadf(io)
  end
  # host;interval;time;runq_sz;plist_sz;ldavg_1;ldavg_5;ldavg_15
  def load_qlen(file)
    io=open_sadf(file, '-q')
    while ln=read_sadf(io)
      @add_qlen.execute(ln[0], ln[2], ln[3].to_i, ln[4].to_i,
                        ln[5,3].map { |i| i.to_f })
    end
    close_sadf(io)
  end
  # host;interval;time;kbmemfree;kbmemused;pcmemused;kbbuffers;kbcached;kbswpfree;kbswpused;pcswpused;kbspcad
  def load_memusage(file)
    io=open_sadf(file, '-r')
    while ln=read_sadf(io)
      @add_memu.execute(ln[0],ln[2], ln[3].to_i, ln[4].to_i,
                        ln[5].to_f, ln[6].to_i, ln[7].to_i, ln[8].to_i,
                        ln[9].to_i, ln[10].to_f, ln[11].to_i)
    end
    close_sadf(io)
  end
  #host;interval;time;frmpg_s;bufpg_s;campg_s
  def load_mem(file)
    io=open_sadf(file, '-R')
    while ln=read_sadf(io)
      @add_mem.execute(ln[0], ln[2], ln[3,3].map { |i| i.to_f })
    end
    close_sadf(io)
  end
  # host;interval;time;file_sz;inode_sz;super_sz;pctsuper_sz;pctdquot_sz;rtsig_sz;pctrtsig_sz
  def load_file(file)
    io=open_sadf(file, '-v')
    while ln=read_sadf(io)
      @add_file.execute(ln[0], ln[2], ln[3,3].map { |i| i.to_i }, ln[6,4].map { |i| i.to_f })
    end
    close_sadf(io)
  end
  #host;interval;time;cswch_s
  def load_ctx(file)
    io=open_sadf(file, '-w')
    while ln=read_sadf(io)
      @add_ctx.execute(ln[0], ln[2], ln[3].to_f)
    end
    close_sadf(io)
  end
  #host;interval;time;pswpin_s;pswpout_s
  def load_swp(file)
    io=open_sadf(file, '-W')
    while ln=read_sadf(io)
      @add_swp.execute(ln[0], ln[2], ln[3].to_f, ln[4].to_f)
    end
    close_sadf(io)
  end

  def saDD_date(file)
    io=IO.popen('sadf -H ' + file)
    io.readline
    system, kernel, hostname, date = io.readline.split
    io.close
    return Time.parse(date)
  end

  def load_saDD(file)
    io=File.open(file)
    digest = Digest::SHA1.new()
    while d=io.read(512)
      digest.update(d)
    end
    io.close
    mtime  = File.mtime(file)
    size   = File.size(file)
    date   = saDD_date(file)
    lfile = @dbh.execute(
      'SELECT "path", "date", "size"
     FROM "loaded_file"
     WHERE "hash" = ?',
     digest
    )

    if lfile.empty?
      l2 = @dbh.execute(
        'SELECT "size"
       FROM "loaded_file"
       WHERE "path" = ? AND "date" = ?',
       file, date.iso8601
      )
      if l2.empty?
        puts "New sar file: #{file}"
        @dbh.execute(
          'INSERT INTO "loaded_file"
          ("hash", "date", "path", "size", "mtime")
          VALUES(?, ?, ?, ?, ?)',
          digest, date.iso8601, file, size, mtime.iso8601
        )
      elsif l2[0][0].to_i < size
        # This would be an updated file
        puts "Updated sar file: #{file}"
        @dbh.execute(
          'UPDATE "loaded_file" SET
         "hash" = ?, "size" = ?, "mtime" = ?
       WHERE "path" = ? AND "date" = ?',
       digest, size, mtime.iso8601, file, date.iso8601
        )
      end
    else
      puts "Already loaded file: #{file}"
      return nil
    end

    load_cpu(file)
    load_paging(file)
    load_intr(file)
    load_proc(file)
    load_disk(file)
    load_net(file)
    load_sock(file)
    load_qlen(file)
    load_memusage(file)
    load_mem(file)
    load_file(file)
    load_ctx(file)
    load_swp(file)
    return true
  end


  def prepare_statements()
    @get_host = @dbh.prepare('SELECT "id" FROM "host" WHERE "hostname" = ?');
    @add_host = @dbh.prepare(
      'INSERT INTO "host" ("hostname", "last_load") VALUES (?, ?)'
    );

    @add_cpu = @dbh.prepare(
      'REPLACE INTO "cpu_stat"
      ("host_id", "time", "cpu", "nice", "system", "iowait", "steal", "idle")
      VALUES( ?, ?, ?, ?, ?, ?, ?, ? )'
    );

    @add_ctx = @dbh.prepare(
      'REPLACE INTO "ctx_stat"
      ("host_id", "time", "cswch_s") VALUES( ?, ?, ? )'
    );

    @add_disk = @dbh.prepare(
      'REPLACE INTO "disk_stat"
      ("host_id", "time", "tps", "rtps", "wtps", "bread_s", "bwrtn_s")
      VALUES( ?, ?, ?, ?, ?, ?, ?)'
    );

    @add_file = @dbh.prepare(
      'REPLACE INTO "file_stat"
      ("host_id", "time", "file_sz", "inode_sz", "super_sz", "pctsuper_sz",
        "pctdquot_sz", "rtsig_sz", "pctrtsig_sz")
      VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)'
    );

    @add_intr = @dbh.prepare(
      'REPLACE INTO "interrupts_stat"
      ("host_id", "time", "intr_s") VALUES (?, ?, ?)'
    );

    @add_mem = @dbh.prepare(
      'REPLACE INTO "mem_stat" ("host_id", "time", "frmpg_s", "bufpg_s", "campg_s") VALUES (?, ?, ?, ?, ?)'
    );

    @add_memu = @dbh.prepare(
      'REPLACE INTO "memusage_stat"
      ("host_id", "time", "kbmemfree", "kbmemused", "pcmemused",
        "kbbuffers", "kbcached", "kbswpfree", "kbswpused", "pcswpused",
        "kbspcad")
      VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
    );

    @add_net = @dbh.prepare(
      'REPLACE INTO "net_stat"
      ("host_id", "time", "iface", "rxpck_s", "txpck_s", "rxbyt_s", "txbyt_s",
        "rxcmp_s", "txcmp_s", "rxmcst_s")
      VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
    );

    @add_paging = @dbh.prepare(
      'REPLACE INTO "paging_stat"
      ("host_id", "time", "pgpgin_s", "pgpgout_s", "fault_s", "majflt_s")
      VALUES(?, ?, ?, ?, ? ,?)'
    );

    @add_proc = @dbh.prepare(
      'REPLACE INTO "proc_stat" ("host_id", "time", "proc_s")
      VALUES (?, ?, ?)'
    );

    @add_qlen = @dbh.prepare(
      'REPLACE INTO "qlen_stat" ("host_id", "time", "runq_sz", "plist_sz", "ldavg_1", "ldavg_5", "ldavg_15") VALUES (?, ?, ?, ?, ?, ?, ?)'
    );

    @add_sock = @dbh.prepare(
      'REPLACE INTO "sock_stat"
      ("host_id", "time", "totsck", "tcpsck", "udpsck", "rawsck", "ip_frag")
      VALUES (?, ?, ?, ?, ?, ?, ?)'
    );

    @add_swp = @dbh.prepare(
      'REPLACE INTO "swp_stat"
      ("host_id", "time", "swpin_s", "swpout_s") VALUES (?, ?, ?, ?)'
    );
  end

  def close_statements()
    @get_host.close
    @add_host.close
    @add_cpu.close
    @add_ctx.close
    @add_disk.close
    @add_file.close
    @add_intr.close
    @add_mem.close
    @add_memu.close
    @add_net.close
    @add_paging.close
    @add_proc.close
    @add_qlen.close
    @add_sock.close
    @add_swp.close
  end

  def get_host(hostname)
    r = @get_host.execute!(hostname)
    if r.nil? or r.empty?
      @add_host.execute!(hostname, nil)
      return @dbh.last_insert_row_id()
    else
      return r[0][0].to_i
    end
  end

  def make_datafile()
    @dbh.transaction
    @dbh.execute_batch(%q{
    /* files that have been loaded into
       this perf-db.
       hash is the SHA256 of the file
       date is the day that the datafile handles
       path is the original path of the file
    */
    CREATE TABLE IF NOT EXISTS loaded_file (
      "hash" BLOB,
      "date" DATE,
      "path" TEXT,
      "size" INTEGER,
      "mtime" DATETIME
    );

    -- hosts that this perf-db knows
    -- id is used as a FK in the stat tables
    CREATE TABLE IF NOT EXISTS host (
      "id" INTEGER PRIMARY KEY AUTOINCREMENT,
      "hostname" TEXT,
      "last_load" DATE
    );

    -- sar option: -I SUM
    CREATE TABLE IF NOT EXISTS interrupts_stat (
      "host_id" INTEGER,
      "time"    DATETIME,
      "intr_s"  REAL
    );

    -- sar option: -B
    CREATE TABLE IF NOT EXISTS paging_stat (
      "host_id" INTEGER,
      "time"    DATETIME,
      "pgpgin_s"  REAL,
      "pgpgout_s"  REAL,
      "fault_s" REAL,
      "majflt_s" REAL
    );

    -- sar option: -c
    CREATE TABLE IF NOT EXISTS proc_stat (
      "host_id" INTEGER,
      "time"    DATETIME,
      "proc_s"  REAL
    );

    -- sar option: -b
    CREATE TABLE IF NOT EXISTS disk_stat (
      "host_id" INTEGER,
      "time"    DATETIME,
      "tps" REAL,
      "rtps" REAL,
      "wtps" REAL,
      "bread_s" REAL,
      "bwrtn_s" REAL
    );

    -- sar option -d not tracked since it's not default

    -- sar option: -n DEV
    CREATE TABLE IF NOT EXISTS net_stat (
      "host_id" INTEGER,
      "time"    DATETIME,
      "iface"   VARCHAR(5),
      "rxpck_s" REAL,
      "txpck_s" REAL,
      "rxbyt_s" REAL,
      "txbyt_s" REAL,
      "rxcmp_s" REAL,
      "txcmp_s" REAL,
      "rxmcst_s" REAL
    );

    -- sar option: -n SOCK
    CREATE TABLE IF NOT EXISTS sock_stat (
      "host_id" INTEGER,
      "time"    DATETIME,
      "totsck"  INTEGER,
      "tcpsck"  INTEGER,
      "udpsck"  INTEGER,
      "rawsck"  INTEGER,
      "ip_frag" INTEGER
    );

    -- sar option: -P ALL
    CREATE TABLE IF NOT EXISTS cpu_stat (
      "host_id" INTEGER,
      "time"    DATETIME,
      "cpu"     INTEGER,
      "nice"  REAL,
      "system"  REAL,
      "iowait"  REAL,
      "steal"  REAL,
      "idle" REAL
    );

    -- sar option: -q
    CREATE TABLE IF NOT EXISTS qlen_stat (
      "host_id" INTEGER,
      "time"    DATETIME,
      "runq_sz"  INTEGER,
      "plist_sz" INTEGER,
      "ldavg_1"  REAL,
      "ldavg_5"  REAL,
      "ldavg_15" REAL
    );

    -- sar option: -r
    CREATE TABLE IF NOT EXISTS memusage_stat (
      "host_id" INTEGER,
      "time"    DATETIME,
      "kbmemfree" INTEGER,
      "kbmemused" INTEGER,
      "pcmemused" REAL,
      "kbbuffers" INTEGER,
      "kbcached"  INTEGER,
      "kbswpfree" INTEGER,
      "kbswpused" INTEGER,
      "pcswpused" REAL,
      "kbspcad" INTEGER
    );

    -- sar option: -R
    CREATE TABLE IF NOT EXISTS mem_stat (
      "host_id" INTEGER,
      "time" DATETIME,
      "frmpg_s" REAL,
      "bufpg_s" REAL,
      "campg_s" REAL
    );

    -- sar option: -v
    CREATE TABLE IF NOT EXISTS file_stat (
      "host_id" INTEGER,
      "time" DATETIME,
      "file_sz" INTEGER,
      "inode_sz" INTEGER,
      "super_sz" INTEGER,
      "pctsuper_sz" REAL,
      "pctdquot_sz" REAL,
      "rtsig_sz" REAL,
      "pctrtsig_sz" REAL
    );

    -- sar option: -w
    CREATE TABLE IF NOT EXISTS ctx_stat (
      "host_id" INTEGER,
      "time"    DATETIME,
      "cswch_s"  REAL
    );

    -- sar option: -W
    CREATE TABLE IF NOT EXISTS swp_stat (
      "host_id" INTEGER,
      "time"    DATETIME,
      "swpin_s"  REAL,
      "swpout_s"  REAL
    );

    CREATE UNIQUE INDEX IF NOT EXISTS intr_host_time
      ON interrupts_stat ("host_id", "time");
    CREATE UNIQUE INDEX IF NOT EXISTS pag_host_time
      ON paging_stat ("host_id", "time");
    CREATE UNIQUE INDEX IF NOT EXISTS disk_host_time
      ON disk_stat ("host_id", "time");
    CREATE UNIQUE INDEX IF NOT EXISTS net_host_time_iface
      ON net_stat ("host_id", "time", "iface");
    CREATE UNIQUE INDEX IF NOT EXISTS sock_host_time
      ON sock_stat ("host_id", "time");
    CREATE UNIQUE INDEX IF NOT EXISTS cpu_host_time_cpu
      ON cpu_stat ("host_id", "time", "cpu");
    CREATE UNIQUE INDEX IF NOT EXISTS memu_host_time
      ON memusage_stat ("host_id", "time");
    CREATE UNIQUE INDEX IF NOT EXISTS mem_host_time
      ON mem_stat ("host_id", "time");
    CREATE UNIQUE INDEX IF NOT EXISTS file_host_time
      ON file_stat ("host_id", "time");
    CREATE UNIQUE INDEX IF NOT EXISTS ctx_host_time
      ON ctx_stat ("host_id", "time");
    CREATE UNIQUE INDEX IF NOT EXISTS swp_host_time
      ON swp_stat ("host_id", "time");
    })
    @dbh.commit
  end

end
