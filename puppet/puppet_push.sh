#!/bin/bash

NO_SYNC=$1

EMAILS="dba@palominodb.com noc@example.com"

REMOTE=$(git ls-remote -h origin | awk '{print $1}')
HEAD=$(git rev-list -n1 HEAD)

if [[ -f /etc/puppet/git/.git/push_lock ]]; then
  echo "Refusing to push due to a push lock being in place."
  echo "This situation needs to be resolved manually."
  echo "This is usually done by making sure all changes are in the master repo,"
  echo "removing /etc/puppet/git/.git/push_lock, and doing a 'git pull'"
  echo "from /etc/puppet/git and ensuring that no errors are encountered."
  exit 1
fi

if [[ "$REMOTE" = "$HEAD" ]]; then
  pushd /etc/puppet/git/ >/dev/null
  echo "Password to do update:"
  sudo git reset --hard HEAD ; sudo git pull -f
  popd >/dev/null
elif [[ "x$NO_SYNC" == "xiamsure" ]]; then

  sudo touch /etc/puppet/git/.git/push_lock
  cat <<MAILEOF | mail -s "`whoami` has made a forced puppet update." $EMAILS
Please be aware that this blocks all further updates until the lock is removed.
The user's configured git user/email are: "`git config user.name`" <`git config user.email`>
MAILEOF

  sudo git push /etc/puppet/git +master:master
  pushd /etc/puppet/git/ >/dev/null
  sudo git reset --hard HEAD
  popd >/dev/null

else
  echo "Refusing to update /etc/puppet/git until origin has our changes."
  echo "You can do this by running 'git push'."
  echo "If you are positive about what you want to do, run like: $0 iamsure"
  echo "This will do a few things."
  echo "  1) It will send an email with a notification about the change."
  echo "     To: $EMAILS"
  echo "  2) It will do push like: git push /etc/puppet/git +master:master"
  echo "  3) It will set an advisory lock in /etc/puppet/git/.git/push_lock"
  echo "This tool will refuse to do any further updates until that lock has been removed."
fi
