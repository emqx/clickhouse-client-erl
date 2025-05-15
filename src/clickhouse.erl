-module(clickhouse).

-behaviour(gen_server).

%% API.
-export([ start_link/0
        , start_link/1
        , stop/1
        ]).

-export ([ query/3
         , query/4
         , execute/3
         , status/1
         , detailed_status/1
         , detailed_status/2
         ]).

%% gen_server.
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        , format_status/1
        , format_status/2
        ]).

-record(state, {url, user, key, pool}).

%% API.
-spec start_link() -> {ok, pid()}.
start_link() ->
    start_link([]).

start_link(Opts) ->
    gen_server:start_link(?MODULE, [Opts], []).

stop(Pid) ->
    gen_server:stop(Pid).

query(Pid, SQL, Opts) ->
    gen_server:call(Pid, {SQL, Opts}, infinity).

query(Pid, SQL, Opts, Timeout) ->
    gen_server:call(Pid, {SQL, Opts}, Timeout).

execute(Pid, SQL, Opts) ->
    gen_server:cast(Pid, {SQL, Opts}).

%% This is kept for backwards compatibility. Most applications should
%% use detailed_status/1 as it provides more information about the status.
-spec status(pid()) -> true | false.
status(Pid) ->
    gen_server:call(Pid, status).

-spec detailed_status(pid()) -> ok | {error, Reason} when
      Reason :: term().
detailed_status(Pid) ->
    detailed_status(Pid, 5_000).

-spec detailed_status(pid(), timeout()) -> ok | {error, Reason} when
      Reason :: term().
detailed_status(Pid, Timeout) ->
    gen_server:call(Pid, detailed_status, Timeout).

%% gen_server.
init([Opts]) ->
    Url0 = proplists:get_value(url, Opts, <<"http://127.0.0.1:8123">>),
    Url = case proplists:get_value(database, Opts) of
        undefined -> Url0;
        DB -> <<Url0/binary, "?database=", DB/binary>>
    end,
    State = #state{url = Url,
                   user = proplists:get_value(user, Opts, <<"default">>),
                   key =  proplists:get_value(key, Opts, <<"123456">>),
                   pool = proplists:get_value(pool, Opts, default)},
    {ok, State}.

handle_call({SQL, _Opts}, _From, State = #state{url = Url, user = User, key =  Key, pool = Pool}) ->
    Reply = query(Pool, Url, User, Key, SQL),
    {reply, Reply, State};

handle_call(status, _From, State = #state{url = Url, user = User, key =  Key, pool = Pool}) ->
    case query(Pool, Url, User, Key, <<"SELECT 1">>) of
        {ok, _, _} -> {reply, true, State};
        _ -> {reply, false, State}
    end;

handle_call(detailed_status, _From, State = #state{url = Url, user = User, key =  Key, pool = Pool}) ->
    case query(Pool, Url, User, Key, <<"SELECT 1">>) of
        {ok, 200, Response} ->
            case erlang:iolist_to_binary(string:trim(Response)) of
               <<"1">> ->
                    {reply, ok, State};
                UnexpectedResponse ->
                    {reply, {error, {unexpected_response, UnexpectedResponse}}, State}
            end;
        Error ->
            {reply, {error, Error}, State}
    end;

handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast({SQL, _Opts}, State = #state{url = Url, user = User, key =  Key, pool = Pool}) ->
    query(Pool, Url, User, Key, SQL),
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

format_status(#{state := State} = Status) ->
    Status#{state := State#state{key = <<"******">>}}.

format_status(_Opt, [_PDict, State]) ->
    [{data, [{"State", State#state{key = <<"******">>}}]}].

query(Pool, Url, User, Key, SQL) ->
    Headers = [{<<"X-ClickHouse-User">>, User},
               {<<"X-ClickHouse-Key">>, Key}],
    Options = [{pool, Pool},
               {connect_timeout, 10000},
               {recv_timeout, 30000},
               {follow_redirectm, true},
               {max_redirect, 5},
               with_body],
    case hackney:request(post, Url, Headers, SQL, Options) of
        {ok, StatusCode, _Headers, ResponseBody}
          when StatusCode =:= 200 orelse StatusCode =:= 204 ->
            {ok, StatusCode, ResponseBody};
        {ok, StatusCode, _Headers, ResponseBody} ->
            {error, {StatusCode, ResponseBody}};
        {error, Reason} ->
            {error, Reason}
    end.
