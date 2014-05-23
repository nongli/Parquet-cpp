// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <stdlib.h>
#include <stdio.h>
#include <iostream>

#include <boost/utility.hpp>
#include <gtest/gtest.h>
#include <math.h>

#include <parquet/parquet.h>
#include <encodings/encodings.h>

using namespace boost;
using namespace impala;
using namespace parquet;
using namespace parquet_cpp;
using namespace std;

#define BUFFER_SIZE (1024 * 1024)

struct EncodeDecode {
  Encoder* e;
  Decoder* d;

  EncodeDecode(Encoder* e, Decoder* d) : e(e), d(d) {}
};

unordered_map<Type::type, vector<EncodeDecode> > all_encodings;

void ToByteArray(const vector<string>& src, vector<ByteArray>* dst) {
  dst->clear();
  dst->resize(src.size());
  for (int i = 0; i < src.size(); ++i) {
    (*dst)[i].len = src[i].size();
    (*dst)[i].ptr = (const uint8_t*)src[i].c_str();
  }
}

template<typename T>
void TestValues(Encoder* e, Decoder* d, const T* values, int num) {
  e->Reset();
  int n  = e->Add(values, num);
  EXPECT_EQ(n, num);

  int encoded_len = 0;
  const uint8_t* encoded = e->Encode(&encoded_len);

  d->SetData(num, encoded, encoded_len);
  T decoded[num];
  n = d->Get(decoded, num);
  EXPECT_EQ(n, num);

  for (int i = 0; i < num; ++i) {
    EXPECT_EQ(decoded[i], values[i]) << i;
  }
}

template<>
void TestValues(Encoder* e, Decoder* d, const string* values, int num) {
  e->Reset();
  for (int i = 0; i < num; ++i) {
    ByteArray ba;
    ba.len = values[i].size();
    ba.ptr = (const uint8_t*)values[i].c_str();
    int n  = e->Add(&ba, 1);
    EXPECT_EQ(n, 1);
  }

  int encoded_len = 0;
  const uint8_t* encoded = e->Encode(&encoded_len);

  d->SetData(num, encoded, encoded_len);
  ByteArray decoded[num];
  int n = d->Get(decoded, num);
  EXPECT_EQ(n, num);

  for (int i = 0; i < num; ++i) {
    string v((const char*)decoded[i].ptr, decoded[i].len);
    EXPECT_EQ(v, values[i]) << i;
  }
}

template<typename T>
void TestAllEncodings(Type::type t, const T* values, int num) {
  const vector<EncodeDecode>& ed = all_encodings[t];
  for (int i = 0; i < ed.size(); ++i) {
    TestValues(ed[i].e, ed[i].d, values, num);
  }
}

TEST(Encoder, BasicTest) {
  int32_t i32_values[] = { -1, 1, 2, 0, 3, 4, 1 };
  int64_t i64_values[] = { -1, 1, 2, 0, 3, 4, 1 };
  float float_values[] = { -1, 1, 2, 0, 3, 4, 1 };
  double double_values[] = { -1, 1, 2, 0, 3, 4, 1 };

  TestAllEncodings(Type::INT32, i32_values, sizeof(i32_values) / sizeof(int32_t));
  TestAllEncodings(Type::INT64, i64_values, sizeof(i64_values) / sizeof(int64_t));
  TestAllEncodings(Type::FLOAT, float_values, sizeof(float_values) / sizeof(float));
  TestAllEncodings(Type::DOUBLE, double_values, sizeof(double_values) / sizeof(double));
}

TEST(BoolEncoder, Basic) {
  scoped_ptr<BoolEncoder> e(new BoolEncoder(BUFFER_SIZE));
  scoped_ptr<BoolDecoder> d(new BoolDecoder());

  const int N = 100000;
  bool v[N];

  // All true.
  for (int i = 0; i < N; ++i) {
    v[i] = true;
  }
  TestValues(e.get(), d.get(), v, N);

  // All false
  for (int i = 0; i < N; ++i) {
    v[i] = false;
  }
  TestValues(e.get(), d.get(), v, N);

  // Alternating
  for (int i = 0; i < N; ++i) {
    v[i] = (i % 2 == 0);
  }
  TestValues(e.get(), d.get(), v, N);

  // every j
  for (int j = 2; j < 20; ++j) {
    for (int i = 0; i < N; ++i) {
      v[i] = (i % j == 0);
    }
    TestValues(e.get(), d.get(), v, N);
  }

  for (int i = 0; i < 100; ++i) {
    const int N = 1000;
    bool v[N];;
    for (int j = 0; j < N; ++j) {
      v[j] = rand() % 2 == 0;
    }
    TestValues(e.get(), d.get(), v, N);
  }
}

TEST(StringEncoder, Basic) {
  vector<string> values;
  // Wikipedia example
  values.push_back("myxa");
  values.push_back("myxophyta");
  values.push_back("myxopod");
  values.push_back("nab");
  values.push_back("nabbed");
  values.push_back("nabbing");
  values.push_back("nabit");
  values.push_back("nabk");
  values.push_back("nabob");
  values.push_back("nacarat");
  values.push_back("nacelle");

  TestAllEncodings(Type::BYTE_ARRAY, &values[0], values.size());
}

void InitEncodings() {
  all_encodings[Type::BOOLEAN].push_back(EncodeDecode(
      new BoolEncoder(BUFFER_SIZE), new BoolDecoder));

  all_encodings[Type::INT32].push_back(EncodeDecode(
      new PlainEncoder(Type::INT32, BUFFER_SIZE), new PlainDecoder(Type::INT32)));
  all_encodings[Type::INT32].push_back(EncodeDecode(
      new DeltaBitPackEncoder(Type::INT32, BUFFER_SIZE),
      new DeltaBitPackDecoder(Type::INT32)));

  all_encodings[Type::INT64].push_back(EncodeDecode(
      new PlainEncoder(Type::INT64, BUFFER_SIZE), new PlainDecoder(Type::INT64)));
  all_encodings[Type::INT64].push_back(EncodeDecode(
      new DeltaBitPackEncoder(Type::INT64, BUFFER_SIZE),
      new DeltaBitPackDecoder(Type::INT64)));

  all_encodings[Type::FLOAT].push_back(EncodeDecode(
      new PlainEncoder(Type::FLOAT, BUFFER_SIZE), new PlainDecoder(Type::FLOAT)));

  all_encodings[Type::DOUBLE].push_back(EncodeDecode(
      new PlainEncoder(Type::DOUBLE, BUFFER_SIZE), new PlainDecoder(Type::DOUBLE)));

  all_encodings[Type::BYTE_ARRAY].push_back(EncodeDecode(
      new PlainEncoder(Type::BYTE_ARRAY, BUFFER_SIZE),
      new PlainDecoder(Type::BYTE_ARRAY)));
  all_encodings[Type::BYTE_ARRAY].push_back(EncodeDecode(
      new DeltaLengthByteArrayEncoder(BUFFER_SIZE),
      new DeltaLengthByteArrayDecoder()));
  all_encodings[Type::BYTE_ARRAY].push_back(EncodeDecode(
      new DeltaByteArrayEncoder(BUFFER_SIZE),
      new DeltaByteArrayDecoder()));

}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  InitEncodings();
  return RUN_ALL_TESTS();
}

