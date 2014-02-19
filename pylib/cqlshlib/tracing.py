# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from cqlshlib.displaying import MAGENTA
from datetime import datetime
import time

TRACING_KS = 'system_traces'
SESSIONS_CF = 'sessions'
EVENTS_CF = 'events'

def print_trace_session(shell, session, session_id):
    rows = fetch_trace_session(session, session_id)
    if not rows:
        shell.printerr("Session %s wasn't found." % session_id)
        return
    names = ['activity', 'timestamp', 'source', 'source_elapsed']

    formatted_names = map(shell.myformat_colname, names)
    formatted_values = [map(shell.myformat_value, row) for row in rows]

    shell.writeresult('')
    shell.writeresult('Tracing session: ', color=MAGENTA, newline=False)
    shell.writeresult(session_id)
    shell.writeresult('')
    shell.print_formatted_result(formatted_names, formatted_values)
    shell.writeresult('')

def fetch_trace_session(session, session_id):
    traces = session.execute("SELECT request, coordinator, started_at, duration "
                   "FROM %s.%s "
                   "WHERE session_id = %s" % (TRACING_KS, SESSIONS_CF, session_id))
    if not traces:
        return []

    trace = traces[0]

    events = session.execute("SELECT activity, event_id, source, source_elapsed "
                   "FROM %s.%s "
                   "WHERE session_id = %s" % (TRACING_KS, EVENTS_CF, session_id))

    # append header row (from sessions table).
    rows = [[trace.request, str(datetime_from_utc_to_local(trace.started_at)), trace.coordinator, 0]]

    # append main rows (from events table).
    for event in events:
        rows.append([event.activity, str(format_timeuuid(event.event_id)), event.source, event.source_elapsed])
    # append footer row (from sessions table).
    if trace.duration:
        from datetime import timedelta
        finished_at = str(datetime_from_utc_to_local(trace.started_at) + timedelta(microseconds=trace.duration))
    else:
        finished_at = trace.duration = "--"

    rows.append(['Request complete', finished_at, trace.coordinator, trace.duration])

    return rows

def format_timeuuid(value):
    from datetime import datetime
    return datetime.fromtimestamp((value.get_time() - 0x01b21dd213814000L)*100/1e9)

def datetime_from_utc_to_local(utc_datetime):
    now_timestamp = time.time()
    offset = datetime.fromtimestamp(now_timestamp) - datetime.utcfromtimestamp(now_timestamp)
    return utc_datetime + offset

