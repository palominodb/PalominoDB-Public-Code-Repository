#
# Fact for mysql socket path
#
Facter.add("mysql_socket") do
  setcode do
    path=nil
    %x{my_print_defaults mysqld}.split("\n").each do |l|
      arg,val = l.split('=')
      if arg == '--socket'
        path=val
        break
      end
    end
    path
  end
end
