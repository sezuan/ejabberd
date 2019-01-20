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
	 extended_fields/0, store/8, write_prefs/4, get_prefs/2, select/6, remove_from_archive/3]).

%% DEBUG EXPORTS
-export([timestamp_to_binary/1, binary_to_timestamp/1]).

-include_lib("stdlib/include/ms_transform.hrl").
-include("xmpp.hrl").
-include("logger.hrl").
-include("mod_mam.hrl").

-define(BIN_GREATER_THAN(A, B),
	((A > B andalso byte_size(A) == byte_size(B))
	 orelse byte_size(A) > byte_size(B))).
-define(BIN_LESS_THAN(A, B),
	((A < B andalso byte_size(A) == byte_size(B))
	 orelse byte_size(A) < byte_size(B))).

-define(TABLE_SIZE_LIMIT, 2000000000). % A bit less than 2 GiB.

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

delete_us({Table, US}) ->
    Msgs = mnesia:match_object(Table, #archive_msg_set{
		us = #ust{us = US, timestamp = '_'}, _ = '_'}, read),
    lists:foreach(fun(X) ->
            mnesia:delete_object(X)
        end, Msgs).

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

select(_LServer, JidRequestor,
       #jid{luser = LUser, lserver = LServer} = JidArchive,
       Query, RSM, MsgType) ->
    Start = proplists:get_value(start, Query),
    End = proplists:get_value('end', Query),
    With = proplists:get_value(with, Query),
    LWith = if With /= undefined -> jid:tolower(With);
	       true -> undefined
	    end,
    MS = make_matchspec(LUser, LServer, Start, End, LWith),
    Msgs = mnesia:dirty_select(archive_msg_set, MS),
    SortedMsgs = lists:sort(
		   fun(A, B) ->
			   #archive_msg_set{us = #ust{timestamp = T1}} = A,
			   #archive_msg_set{us = #ust{timestamp = T2}} = B,
			   T1 =< T2
		   end, Msgs),
    {FilteredMsgs, IsComplete} = filter_by_rsm(SortedMsgs, RSM),
    Count = length(Msgs),
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
		end, FilteredMsgs), IsComplete, Count},
    erlang:garbage_collect(),
    Result.

%%%===================================================================
%%% Internal functions
%%%===================================================================
make_matchspec(LUser, LServer, Start, undefined, With) ->
    %% List is always greater than a tuple
    make_matchspec(LUser, LServer, Start, [], With);
make_matchspec(LUser, LServer, Start, End, {_, _, <<>>} = With) ->
    ets:fun2ms(
      fun(#archive_msg_set{us = #ust{us = US, timestamp = TS},
		           bare_peer = BPeer} = Msg)
	    when Start =< TS, End >= TS,
		 US == {LUser, LServer},
		 BPeer == With ->
	      Msg
      end);
make_matchspec(LUser, LServer, Start, End, {_, _, _} = With) ->
    ets:fun2ms(
      fun(#archive_msg_set{us = #ust{us = US, timestamp = TS},
		           peer = Peer} = Msg)
	    when Start =< TS, End >= TS,
		 US == {LUser, LServer},
		 Peer == With ->
	      Msg
      end);
make_matchspec(LUser, LServer, Start, End, undefined) ->
    ets:fun2ms(
      fun(#archive_msg_set{us = #ust{us = US, timestamp = TS},
		           peer = Peer} = Msg)
	    when Start =< TS, End >= TS,
		 US == {LUser, LServer} ->
	      Msg
      end).

filter_by_rsm(Msgs, undefined) ->
    {Msgs, true};
filter_by_rsm(_Msgs, #rsm_set{max = Max}) when Max < 0 ->
    {[], true};
filter_by_rsm(Msgs, #rsm_set{max = Max, before = Before, 'after' = After}) ->
    NewMsgs = if is_binary(After), After /= <<"">> ->
		      lists:filter(
			fun(#archive_msg_set{us = #ust{timestamp = I}}) ->
				I > binary_to_timestamp(After)
			end, Msgs);
		 is_binary(Before), Before /= <<"">> ->
		      lists:foldl(
			fun(#archive_msg_set{us = #ust{timestamp = I}} = Msg, Acc) ->
			    case I < binary_to_timestamp(Before) of
				true -> [Msg|Acc];
				false -> Acc
			    end
			end, [], Msgs);
		 is_binary(Before), Before == <<"">> ->
		      lists:reverse(Msgs);
		 true ->
		      Msgs
	      end,
    filter_by_max(NewMsgs, Max).

filter_by_max(Msgs, undefined) ->
    {Msgs, true};
filter_by_max(Msgs, Len) when is_integer(Len), Len >= 0 ->
    {lists:sublist(Msgs, Len), length(Msgs) =< Len};
filter_by_max(_Msgs, _Junk) ->
    {[], true}.

-spec timestamp_to_binary(erlang:timestamp()) -> binary().
timestamp_to_binary(Timestamp) ->
    base64:encode(term_to_binary(Timestamp)).

-spec binary_to_timestamp(binary()) -> erlang:timestamp().
binary_to_timestamp(Binary) ->
    case catch binary_to_term(base64:decode(Binary)) of
	{'EXIT', _} -> <<"">>;
	Timestamp -> Timestamp
    end.
