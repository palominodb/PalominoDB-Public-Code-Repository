require 'active_support'
module TTT
  class AppConfigError < NameError ; end
  class AppConfig
    attr_reader :section

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
