# Copyright 2012 Cloudera Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

SET(LINK_LIBS
  ${PARQUET_LIBS}
  ${PARQUET_EXTERNAL_LIBS}
  gtest)

FUNCTION(ADD_UNIT_TEST TEST_NAME)
  ADD_EXECUTABLE(${TEST_NAME} ${TEST_NAME}.cc)
  TARGET_LINK_LIBRARIES(${TEST_NAME} ${LINK_LIBS})
  ADD_TEST(${TEST_NAME} "../test_bin/${TEST_NAME}")
ENDFUNCTION()

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")

ADD_UNIT_TEST(bit-util-test)
ADD_UNIT_TEST(encoding-test)
ADD_UNIT_TEST(rle-test)
