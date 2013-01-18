# 005_ttt_collector.rb
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
