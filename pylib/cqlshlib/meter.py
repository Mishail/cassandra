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

from greplin import scales
from greplin.scales import meter
import time
import sys


class Meter(object):

    _num_finished = scales.IntStat('finished')
    _rows_inserted = meter.MeterStat('rows_inserted')
    _rows_read = meter.MeterStat('rows_read')

    def __init__(self):
        scales.initChild(self, '/meter%s' % time.time())

    def avg_read(self):
        return self._rows_read['m1'] if 'm1' in self._rows_read else 0

    def avg_written(self):
        return self._rows_inserted['m1'] if 'm1' in self._rows_inserted else 0

    def mark_read(self):
        self._rows_read.mark()

    def mark_written(self):
        self.num_finished += 1
        self._rows_inserted.mark()

    @property
    def num_finished(self):
        return self._num_finished

    @num_finished.setter
    def num_finished(self, value):
        self._num_finished = value
        if self.num_finished % 1000 != 0:
            return
        output = 'Processed %s rows; Read: %.2f rows/s; Write: %.2f rows/s\r' % \
                 (self._num_finished,
                  self.avg_read(),
                  self.avg_written()
                  )
        sys.stdout.write(output)
        sys.stdout.flush()
