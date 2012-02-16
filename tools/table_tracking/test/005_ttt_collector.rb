require 'ttt/db'
require 'ttt/collector'
require 'yaml'

describe TTT::CollectorRegistry do
  before :all do
    TTT::CollectorRegistry.unload
  end
  after :all do
    TTT::CollectorRegistry.reload!
  end
  it 'should be empty in the beginning' do
    TTT::CollectorRegistry.all.should == []
  end

  it 'should return a non-empty list after collector registration' do
    c=TTT::Collector.new(NilClass, 'fake collector') {}
    TTT::CollectorRegistry.all.should == [c]
  end

  it 'should only load once' do
    TTT::CollectorRegistry.load.should == true
    TTT::CollectorRegistry.all.length.should > 1
    TTT::CollectorRegistry.load.should == nil
  end

end

describe TTT::CollectorRun do
  include TestDbHelper
  before :all do
    test_connect
    test_connect_is('localhost')
  end

  after :all do
    test_cleanup
  end

  [:definition, :view, :volume, :user].each do |c|
    it "should find #{c}" do
      TTT::CollectorRun.find_by_collector(c).class.should == TTT::CollectorRun
    end

    it "TTT::CollectorRun(#{c}).collector should equal #{c}" do
      TTT::CollectorRun.find_by_collector(c).collector.should == c.to_s
    end

  end
  it "can find using a collector class" do
    TTT::CollectorRun.find_by_collector(TTT::CollectorRegistry.all[0]).class.should == TTT::CollectorRun
  end
end

describe TTT::Collector do
  include TestDbHelper
  before :all do
    test_connect
    test_connect_is('localhost')
  end

  after :all do
    test_cleanup
  end
end

describe TTT::CollectionDirector::TableCache do
  include TestDbHelper
  before :all do
    test_connect
    test_connect_is('localhost')
    test_migration(CreateTestDataTable)
  end

  after :all do
    test_cleanup
  end

  before do
    @cache = TTT::CollectionDirector::TableCache.new(TTT::TABLE.all)
  end
  it "find_by_schema_and_table should find `test`.`test_data`" do
    t=@cache.find_by_schema_and_table("test", "test_data")
    t.class.should == TTT::TABLE
    t.should_not == nil
    t.schema.should == "test"
    t.name.should == "test_data"
  end
end
