# Be sure to restart your server when you modify this file.

# Your secret key for verifying cookie session data integrity.
# If you change this key, all old sessions will become invalid!
# Make sure the secret is at least 30 characters and all random, 
# no regular words or you'll be exposed to dictionary attacks.
ActionController::Base.session = {
  :key         => '_ttt_gui_session',
  :secret      => '29e9c14e30a0abb10f1ec3545bbb782882ff078c8bc83def44b098835da71302bb2a4aba15b9ae5ff4f612b883d5bc2fe87bcda764196bf76d7a2641569705b9'
}

# Use the database for sessions instead of the cookie-based default,
# which shouldn't be used to store highly confidential information
# (create the session table with "rake db:sessions:create")
# ActionController::Base.session_store = :active_record_store
