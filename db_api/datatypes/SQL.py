# Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# This currently only holds integer types, because that's all the tables
# require.  Can add string types and the like when they are necessary.
#
# pylint: disable=invalid-name
"""
Simple dictionary for information about mysql data types.
Used for schema construction and data validation.
More information:
  http://dev.mysql.com/doc/refman/5.7/en/data-types.html
"""
types = {
    "TINYINT": {'bytes': 1,
                'max_value': 255},  # 8-bit  uint8
    "SMALLINT": {'bytes': 2,
                 'max_value': 65535},  # 16-bit uint16
    "MEDIUMINT": {'bytes': 3,
                  'max_value': 16777215},  # 24-bit uint24
    "INT": {'bytes': 4,
            'max_value': 4294967295},  # 32-bit uint32
    "BIGINT": {'bytes': 8,
               'max_value': 18446744073709551615},  # 64-bit uint64
}
