http_get=$(which curl 2>/dev/null)

if [[ -z "$http_get" ]]; then
  http_get=$(which wget 2>/dev/null)
fi

if [[ -z "$http_get" ]]; then
  echo Unable to find wget or curl needed for nagios_downtime.sh hook.
  exit 3
fi

if [[ -z "$nagios_url" ]]; then
  echo Unable to $(basename $http_get) an empty url. Please set nagios_url in myctl.cnf
  exit 3
fi

if [[ -z "$nagios_downtime_length" ]]; then
  nagios_downtime_length=600
fi


hook_start() {
  result=$1
  if [[ "$result"="started" ]]; then
    $http_get "${nagios_url}/downtime.cgi?host=$(hostname)&action=stop"
    echo
  fi
}

hook_stop() {
  result=$1
  if [[ "$result"="stopped" ]]; then
    $http_get "${nagios_url}/downtime.cgi?host=$(hostname)&action=start&who=mysqlctl&comment=mysqlctl%20stop&service=MySQL&minutes=$nagios_downtime_length"
    echo
  fi
}

