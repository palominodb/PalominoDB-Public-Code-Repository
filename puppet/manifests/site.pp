import "nodes/*"

# When DNS is available this should be changed to the DNS name.
filebucket {
  main: server => "10.10.1.20"
}

File { backup => main }
