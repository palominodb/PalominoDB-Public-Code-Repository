# config.rb
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

#    protected
    class_inheritable_accessor :top

    public
    def initialize(hsh, section=nil)
      @section = section
      @values=hsh
      @top = hsh if(section.nil?)
    end

    def [](x)
      if @values[x].class == Hash
        AppConfig.new(@values[x], x)
      else
        @values[x]
      end
    end

    def need_section(s)
      unless @values.key? s and @values[s].class == Hash
        raise AppConfigError, "Missing required section: #{s}"
      end
      self[s]
    end

    def need_option(key)
      unless @values.key? key
        raise AppConfigError, "Missing required option: #{section}.#{key}"
      end
      @values[key]
    end

    def want_option(key, default=nil)
      unless @values[key]
        default
      else
        @values[key]
      end
    end

    def inspect
      "AppConfig<#{section ? section + ": " : ""}#{@values.keys.join(', ')}>"
    end
  end
end
