%%%-------------------------------------------------------------------
%%% File    : mod_mam_mnesia.erl
%%% Author  : Evgeny Khramtsov <ekhramtsov@process-one.net>
%%% Created : 15 Apr 2016 by Evgeny Khramtsov <ekhramtsov@process-one.net>
%%%
%%%
%%% ejabberd, Copyright (C) 2002-2019   ProcessOne
%%%
%%% This program is free software; you can redistribute it and/or
%%% modify it under the terms of the GNU General Public License as
%%% published by the Free Software Foundation; either version 2 of the
%%% License, or (at your option) any later version.
%%%
%%% This program is distributed in the hope that it will be useful,
%%% but WITHOUT ANY WARRANTY; without even the implied warranty of
%%% MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
%%% General Public License for more details.
%%%
%%% You should have received a copy of the GNU General Public License along
%%% with this program; if not, write to the Free Software Foundation, Inc.,
%%% 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
%%%
%%%----------------------------------------------------------------------

-module(mod_mam_leveldb).

-behaviour(mod_mam).

%% API
-export([init/2, remove_user/2, remove_room/3, delete_old_messages/3,
	 extended_fields/0, store/8, write_prefs/4, get_prefs/2, select/6, remove_from_archive/3,
	 convert_to_leveldb/0]).

%% DEBUG EXPORTS
-export([timestamp_to_binary/1, binary_to_timestamp/1,
	 fetch_next_key/1, fetch_next_key/2,
	 delete_us/1,
	 select_from_leveldb/6, inc_timestamp/1, dec_timestamp/1]).

-include_lib("stdlib/include/ms_transform.hrl").
-include("xmpp.hrl").
-include("logger.hrl").
-include("mod_mam.hrl").

-define(DEF_PAGE_SIZE, 50).

-define(BIN_GREATER_THAN(A, B),
	((A > B andalso byte_size(A) == byte_size(B))
	 orelse byte_size(A) > byte_size(B))).
-define(BIN_LESS_THAN(A, B),
	((A < B andalso byte_size(A) == byte_size(B))
	 orelse byte_size(A) < byte_size(B))).

-define(TABLE_SIZE_LIMIT, 2000000000). % A bit less than 2 GiB.

-record(search_options, {
	direction, start, stop, user, server, with, max}).

%%%===================================================================
%%% Should be exported as COMMANDs
%%%===================================================================
convert_to_leveldb() ->
    Keys = mnesia:dirty_all_keys(archive_msg),
    lists:foreach(
      fun(X) ->
	      Items = mnesia:dirty_read(archive_msg, X),
	      lists:foreach(
		fun(Y) ->
			#archive_msg{us= US, timestamp= Timestamp,
				     peer= Peer, bare_peer = BarePeer,
				     packet = Packet, nick = Nick,
				     type = Type} = Y,
			mnesia:dirty_write(
			  #archive_msg_set{
			     us = #ust{us = US, timestamp = Timestamp},
			     peer = Peer, bare_peer = BarePeer,
			     packet = Packet, nick = Nick,
			     type = Type }
			 )
		end, Items)
      end, Keys).

%%%===================================================================
%%% API
%%%===================================================================
init(_Host, _Opts) ->
    mnesia_eleveldb:register(),
    try
	{atomic, _} = ejabberd_mnesia:create(
			?MODULE, archive_msg_set,
			[{leveldb_copies, [node()]},
			 {attributes, record_info(fields, archive_msg_set)}]),
	{atomic, _} = ejabberd_mnesia:create(
			?MODULE, archive_prefs,
			[{disc_only_copies, [node()]},
			 {attributes, record_info(fields, archive_prefs)}]),
	ok
    catch _:{badmatch, _} ->
	    {error, db_failure}
    end.

delete_us({_Table, {LUser, LServer}}) ->
    SearchOptions= #search_options{
		      direction= {next, fun mnesia:dirty_next/2},
		      start= '_', stop= '_',
		      user= LUser, server= LServer,
		      with= '_', max= '_'},
    search_and_delete(SearchOptions, fetch_next_key(SearchOptions), 0).

search_and_delete(_SearchOptions, '$end_of_table', Count) -> {ok, Count};
search_and_delete(#search_options{user= LUser, server= LServer}= SO,
		  #ust{us= {LUser, LServer}}= Key, Count) ->
    mnesia:dirty_delete(archive_msg_set, Key),
    search_and_delete(SO, fetch_next_key(SO, Key), Count+1);
search_and_delete(_, _, Count) -> {ok, Count}.

remove_user(LUser, LServer) ->
    US = {LUser, LServer},
    F = fun () ->
		delete_us({archive_msg_set, US}),
		mnesia:delete({archive_prefs, US})
	end,
    mnesia:transaction(F).

remove_room(_LServer, LName, LHost) ->
    remove_user(LName, LHost).

remove_from_archive(LUser, LServer, none) ->
    US = {LUser, LServer},
    case mnesia:transaction(fun () -> delete_us({archive_msg_set, US}) end) of
	{atomic, _} -> ok;
	{aborted, Reason} -> {error, Reason}
    end;
remove_from_archive(LUser, LServer, WithJid) ->
    US = {LUser, LServer},
    Peer = jid:remove_resource(jid:split(WithJid)),
    F = fun () ->
	    Msgs = mnesia:match_object(#archive_msg_set{us = #ust{us = US, _ = '_'}, bare_peer = Peer, _ = '_'}),
	    lists:foreach(fun mnesia:delete_object/1, Msgs)
	end,
    case mnesia:transaction(F) of
	{atomic, _} -> ok;
	{aborted, Reason} -> {error, Reason}
    end.

delete_old_messages(global, TimeStamp, Type) ->
    delete_old_user_messages(mnesia:dirty_first(archive_msg_set), TimeStamp, Type).

delete_old_user_messages('$end_of_table', _TimeStamp, _Type) ->
    ok;
delete_old_user_messages(User, TimeStamp, Type) ->
    F = fun() ->
		case mnesia:read(archive_msg_set, User) of
			[#archive_msg_set{us = #ust{timestamp = MsgTS},
					  type = MsgType}] when MsgTS < TimeStamp andalso
							   (Type =:= all orelse
							    Type =:= MsgType) ->
                           mnesia:delete({archive_msg_set, User});
                        _ -> ok
	        end
	end,
    NextRecord = mnesia:dirty_next(archive_msg_set, User),
    case mnesia:transaction(F) of
	{atomic, ok} ->
	    delete_old_user_messages(NextRecord, TimeStamp, Type);
	{aborted, Err} ->
	    ?ERROR_MSG("Cannot delete old MAM messages: ~s", [Err]),
	    Err
    end.

extended_fields() ->
    [].

store(Pkt, _, {LUser, LServer}, Type, Peer, Nick, _Dir, TS) ->
    case {mnesia:table_info(archive_msg_set, disc_only_copies),
	  mnesia:table_info(archive_msg_set, memory)} of
	{[_|_], TableSize} when TableSize > ?TABLE_SIZE_LIMIT ->
	    ?ERROR_MSG("MAM archives too large, won't store message for ~s@~s",
		       [LUser, LServer]),
	    {error, overflow};
	_ ->
	    LPeer = {PUser, PServer, _} = jid:tolower(Peer),
	    F = fun() ->
			mnesia:write(
			  #archive_msg_set{us = #ust{us = {LUser, LServer},
						     timestamp = misc:usec_to_now(TS)},
				           peer = LPeer,
				           bare_peer = {PUser, PServer, <<>>},
				           type = Type,
				           nick = Nick,
				           packet = Pkt})
		end,
	    case mnesia:transaction(F) of
		{atomic, ok} ->
		    ok;
		{aborted, Err} ->
		    ?ERROR_MSG("Cannot add message to MAM archive of ~s@~s: ~s",
			       [LUser, LServer, Err]),
		    Err
	    end
    end.

write_prefs(_LUser, _LServer, Prefs, _ServerHost) ->
    mnesia:dirty_write(Prefs).

get_prefs(LUser, LServer) ->
    case mnesia:dirty_read(archive_prefs, {LUser, LServer}) of
	[Prefs] ->
	    {ok, Prefs};
	_ ->
	    error
    end.

change_record_type(Record, NewType) ->
    [_, US|R] = tuple_to_list(Record),
    #ust{timestamp = Timestamp} = US,
    Id = timestamp_to_binary(Timestamp),
    list_to_tuple([NewType,US,Id,Timestamp] ++ R).

%%%%%%%%%%%%%%%%%%%%%%%%%
%%% New LevelDB Query %%%
%%%%%%%%%%%%%%%%%%%%%%%%%

% Helper Function to Use before/after and start/end identical
-spec inc_timestamp(erlang:timestamp()) -> erlang:timestamp().
inc_timestamp(T) ->
    integer_to_timestamp(timestamp_to_integer(T) + 1).
-spec dec_timestamp(erlang:timestamp()) -> erlang:timestamp().
dec_timestamp(T) ->
    integer_to_timestamp(timestamp_to_integer(T) - 1).

-spec timestamp_to_integer(erlang:timestamp()) -> integer().
timestamp_to_integer({A,B,C}) ->
    A*1000000000000+B*1000000+C.
-spec integer_to_timestamp(integer()) -> erlang:timestamp().
integer_to_timestamp(Timestamp) ->
    {Timestamp div 1000000000000, 
     Timestamp div 1000000 rem 1000000,
     Timestamp rem 1000000}.

select_from_leveldb(LUser, LServer, Start, End, LWith,
		    #rsm_set{'after' = After,
			     before = Before,
			     max = Max}) ->


    % Canonicalize Search Parameter
    Start1 = case {Start, After1 = binary_to_timestamp(After)} of
		 {undefined, undefined} -> {0,0,0};
		 {Start, undefined} -> dec_timestamp(Start);
		 {undefined, After1} -> After1;
		 {Start, After1} when Start =< After1 -> After1;
		 _ -> dec_timestamp(Start)
	     end,
    End1 = case {End, Before1 = binary_to_timestamp(Before)} of
		 {undefined, undefined} -> {9999,0,0};
		 {End, undefined} -> inc_timestamp(End);
		 {undefined, Before1} -> Before1;
		 {End, Before1} when End >= Before1 -> Before1;
	         _ -> inc_timestamp(End)
	     end,

    % Search Direction
    SearchOptions = case Before of
			Before when Before =/= undefined ->
			    #search_options{direction={prev, fun mnesia:dirty_prev/2},
					    start=End1, stop=Start1,
					    user=LUser, server=LServer,
					    with=LWith, max=Max};
			_ -> 
			    #search_options{direction={next, fun mnesia:dirty_next/2},
					    start=Start1, stop=End1,
					    user=LUser, server=LServer,
					    with=LWith, max=Max}
		    end,
    
    Key = fetch_next_key(SearchOptions),
    select_from_leveldb_(SearchOptions,  Key, []).

fetch_next_key(#search_options{user=LUser, server=LServer, start=Start}= SearchOptions) ->
    Key = #ust{us = {LUser, LServer},
	       timestamp = Start},
    fetch_next_key(SearchOptions, Key).

fetch_next_key(#search_options{direction={_, F}}, #ust{us = {LUser, LServer}}= Key) ->
    case F(archive_msg_set, Key) of
	    #ust{us = {LUser, LServer}} = NextKey -> NextKey;
	    _ -> '$end_of_table'
    end.

select_from_leveldb_(#search_options{max=Max}, _, Result)
  when length(Result) =:= Max + 1->
    [_|Result1] = Result,
    {Result1, false};
select_from_leveldb_(_, '$end_of_table', Result) -> {Result, true};
select_from_leveldb_(#search_options{user=User, server=Server},
		     #ust{us={User1,Server1}}, Result)
  when User =/= User1 orelse Server =/= Server1-> {Result, true};
select_from_leveldb_(#search_options{direction={Dir, _}, stop=Ts},
		     #ust{timestamp=Ts1}, Result)
  when (Ts =< Ts1 andalso Dir =:= next) orelse
       (Ts >= Ts1 andalso Dir =:= prev) -> {Result, true};
select_from_leveldb_(#search_options{}= SearchOptions, Key, Result) ->
    [R] = mnesia:dirty_read(archive_msg_set, Key),
    NextResult = filter_with(SearchOptions, R, Result),

    NextKey = fetch_next_key(SearchOptions, Key),
    select_from_leveldb_(SearchOptions, NextKey, NextResult).


filter_with(#search_options{with = undefined}, R, Acc) -> [R|Acc];
filter_with(#search_options{with = {_, _, <<>>} = With}, R, Acc) ->
    case R#archive_msg_set.bare_peer =:= With of
	true -> [R|Acc];
	false -> Acc
    end;
filter_with(#search_options{with = {_, _, _} = With}, R, Acc) ->
    case R#archive_msg_set.peer =:= With of
	true -> [R|Acc];
	false -> Acc
    end.



select(_LServer, JidRequestor,
       #jid{luser = LUser, lserver = LServer} = JidArchive,
       Query, RSM, MsgType) ->
    Start = proplists:get_value(start, Query),
    End = proplists:get_value('end', Query),
    With = proplists:get_value(with, Query),
    LWith = if With /= undefined -> jid:tolower(With);
	       true -> undefined
	    end,
    LRSM = case RSM of
	       undefined -> #rsm_set{max=?DEF_PAGE_SIZE};
	       _ -> RSM
	   end,
    {Msgs, IsComplete} = select_from_leveldb(LUser, LServer,Start, End, LWith, LRSM),

    SortedMsgs = lists:sort(
		   fun(A, B) ->
			   #archive_msg_set{us = #ust{timestamp = T1}} = A,
			   #archive_msg_set{us = #ust{timestamp = T2}} = B,
			   T1 =< T2
		   end, Msgs),

    Count = undefined,
    Result = {lists:flatmap(
		fun(MsgOR) ->
			Msg = change_record_type(MsgOR, archive_msg),
			case mod_mam:msg_to_el(
			       Msg, MsgType, JidRequestor, JidArchive) of
			    {ok, El} ->
				[{Msg#archive_msg.id,
				  Msg#archive_msg.id,
				  El}];
			    {error, _} ->
				[]
			end
		end, SortedMsgs), IsComplete, Count},
    erlang:garbage_collect(),
    Result.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec timestamp_to_binary(erlang:timestamp()) -> binary().
timestamp_to_binary(Timestamp) ->
    integer_to_binary(misc:now_to_usec(Timestamp)).

-spec binary_to_timestamp(binary()) -> erlang:timestamp() | undefined.
binary_to_timestamp(Binary) ->
    case catch misc:usec_to_now(binary_to_integer(Binary)) of
	{'EXIT', _} -> undefined;
	Timestamp -> Timestamp
    end.
