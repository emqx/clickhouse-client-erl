-module(clickhouse).

-behaviour(gen_server).

%% API.
-export([ start_link/0
        , start_link/1
        , stop/1
        ]).

% -export ([ insert/2
%          , insert/3
%          , query/2
%          , query/3
%          ]).
-export ([ insert/4
         ]).

%% gen_server.
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-record(state, {}).

%% API.
-spec start_link() -> {ok, pid()}.
start_link() ->
    start_link([]).

start_link(Opts) ->
    gen_server:start_link(?MODULE, [Opts], []).

stop(Pid) ->
    gen_server:stop(Pid).

insert(Url, Username, Password, SQL) ->
    Headers = [{<<"X-ClickHouse-User">>, Username},
               {<<"X-ClickHouse-Key">>, <<"123456">>}],
    Options = [{pool, default},
               {connect_timeout, 10000},
               {recv_timeout, 30000},
               {follow_redirectm, true},
               {max_redirect, 5},
               with_body],
    Res = hackney:request(post, Url, Headers, SQL, Options),
    io:format("---~p~n", [Res]).

%% gen_server.

init([Opts]) ->
    State = #state{},
    {ok, State}.

handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.