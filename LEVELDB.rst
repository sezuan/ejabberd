
mod_mam_leveldb:select_from_leveldb(<<"matthias">>, <<"localhost">>, {0,0,0}, {9999,0,0},<<"evalocalhost">>,  #rsm_set{max=10}).


rr("/usr/local/src/ejabberd/deps/xmpp/include/xmpp_codec.hrl").

mod_mam_leveldb:select_from_leveldb(<<"matthias">>, <<"localhost">>, {0,0,0}, {9999,0,0},undefined,  #rsm_set{max=2}).

mod_mam_leveldb:select_from_leveldb(<<"matthias">>, <<"localhost">>, {0,0,0}, {9999,0,0},undefined,  #rsm_set{'after' = mod_mam_leveldb:timestamp_to_binary({1548,97213,713078}), max=4}).

mod_mam_leveldb:select_from_leveldb(<<"matthias">>, <<"localhost">>,  {1548,45887,689261}, {1548,45887,711962},undefined,  #rsm_set{max=20}).

