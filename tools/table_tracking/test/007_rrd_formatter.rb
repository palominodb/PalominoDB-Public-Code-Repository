require 'ttt/db'
require 'ttt/collector'
require 'ttt/formatters'
require 'ttt/format/rrd'
require 'test/lib/test_db'
require 'fileutils'
require 'yaml'

# This table is SPECIFICALLY created as MyISAM
# For the more repeatable space accounting provided.
class RrdTestTable < TestMigration
  def self.up
    create_table('test.rrd_test', :options => 'engine=MyISAM') do |t|
      t.string :name, :limit => 5
      t.string :value, :limit => 100
    end
  end

  def self.down
    drop_table('test.rrd_test')
  end
end


describe TTT::RRDFormatter do
  before :all do
    TTT::CollectorRegistry.load
    @volume_collector = nil
    ObjectSpace.each_object() { |o| @volume_collector=o if o.instance_of? TTT::Collector and o.stat == TTT::TableVolume }
  end
  include TestDbHelper
  before do
    test_connect
    test_connect_is('localhost')
    @rrdf = TTT::RRDFormatter.new($stdout, @ttt_config)
    @rrd_io = IO.popen('rrdtool -', 'r+')
  end

  def rrd_lastupdated(rrd, s=-1,e=1)
    @rrd_io.puts "lastupdate #{@ttt_config['formatter_options']['rrd']['path']}/#{rrd}"
    outs=""
    @rrd_io.readpartial(4096, outs)
    outs=outs.split "\n"
    outs[-1].should =~ /^OK u:\d+.\d+ s:\d+.\d+ r:\d+.\d+$/
    outs[s,e]
  end

  def rrd_fetch(rrd, cf, st, et)
    @rrd_io.puts "fetch #{@ttt_config['formatter_options']['rrd']['path']}/#{rrd} #{cf} -s #{st.to_i-60} -e #{st.to_i}"
    outs=""
    @rrd_io.readpartial(4096, outs)
    outs=outs.split "\n"
    outs[-1].should =~ /^OK u:\d+.\d+ s:\d+.\d+ r:\d+.\d+$/
    l=outs.length-3
    outs[-outs.length+2,l]
  end

  after do
    FileUtils.rm_rf(@ttt_config['formatter_options']['rrd']['path']) unless ENV['NO_REMOVE_RRDS']
    test_cleanup
    @rrd_io.puts('quit')
    @rrd_io.close
  end

  def run_collection(id, truth, at_time=Time.at(0))
    cd=TTT::CollectionDirector.new(@ttt_config, at_time)
    cd.stub!(:recache_tables!).and_return {
      cd.instance_variable_set("@cached_tables",
                               TTT::CollectionDirector::TableCache.new(
                                 TTT::TABLE.get('test', 'rrd_test')
                               )
                              )
    }
    rd=cd.collect('localhost', @volume_collector)
    rd.changed?.should == truth
    rd.save(id)
    rd
  end

  it 'should report 0 data_length for server localhost' do
    test_migration(RrdTestTable)
    rd=run_collection(0, true, TIMES[0])
    @rrdf.format([])
    r=rrd_lastupdated("localhost/server_localhost.rrd", -2, 1)
    r.should == ["#{TIMES[0].to_i}: 0 1024 U"]
  end

  it 'should report 0 data_length for database test' do
    test_migration(RrdTestTable)
    rd=run_collection(0, true, TIMES[0])
    @rrdf.format([])
    r=rrd_lastupdated("localhost/database_test.rrd", -2, 1)
    r.should == ["#{TIMES[0].to_i}: 0 1024 U"]
  end

  it 'should report 0 data_length for table rrd_test' do
    test_migration(RrdTestTable)
    rd=run_collection(0, true, TIMES[0])
    @rrdf.format([])
    r=rrd_lastupdated("localhost/test/rrd_test.rrd", -2, 1)
    r.should == ["#{TIMES[0].to_i}: 0 1024 0"]
  end

  it 'should report 0 average for rrd_test with no data' do
    test_migration(RrdTestTable)
    rd=run_collection(0, true, TIMES[0])
    rd=run_collection(1, true, TIMES[1])
    rd=run_collection(2, true, TIMES[2])
    @rrdf.format([])
    r=rrd_fetch("localhost/test/rrd_test.rrd", 'AVERAGE', TIMES[0], TIMES[2])
    r.should == ["#{TIMES[1].to_i-300.seconds}: 0.0000000000e+00 1.0240000000e+03 0.0000000000e+00"]
  end

  # XXX: Why does rrdtool lock this to 1600 hours, but then
  # XXX: won't accept entries that are at exactly that? Fishy.
  it 'should report XX average for rrd_test with 5k rows' do
    test_migration(RrdTestTable)
    load_data('007/5k_rows.dat', 'test', 'rrd_test')
    rd=run_collection(0, true, TIMES[0])
    rd=run_collection(1, true, TIMES[1])
    rd=run_collection(2, true, TIMES[2])
    rd=run_collection(3, true, TIMES[3])
    rd=run_collection(4, true, TIMES[4])
    rd=run_collection(5, true, TIMES[5])
    rd=run_collection(6, true, TIMES[6])
    rd=run_collection(7, true, TIMES[8])
    @rrdf.format([])
    r=rrd_fetch("localhost/test/rrd_test.rrd", 'AVERAGE', TIMES[2]-300.seconds, TIMES[2]-300.seconds)
    r.should == ["#{TIMES[3].to_i-300.seconds}: 1.8000000000e+05 5.3248000000e+04 0.0000000000e+00"]
  end

  it 'handle nil table results' do
    test_migration(RrdTestTable)
    load_data('007/5k_rows.dat', 'test', 'rrd_test')
    rd=run_collection(0, true, TIMES[0])
    rd=run_collection(1, true, TIMES[1])
    test_unmigrate(RrdTestTable)
    rd=run_collection(2, true, TIMES[2])

    @rrdf.format([])
  end
end
