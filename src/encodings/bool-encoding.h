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

#ifndef PARQUET_BOOL_ENCODING_H
#define PARQUET_BOOL_ENCODING_H

#include "encodings.h"

namespace parquet_cpp {

class BoolDecoder : public Decoder {
 public:
  BoolDecoder() : Decoder(parquet::Type::BOOLEAN, parquet::Encoding::PLAIN) { }

  virtual void SetData(int num_values, const uint8_t* data, int len) {
    num_values_ = num_values;
    decoder_ = impala::RleDecoder(data, len, 1);
  }

  virtual int Get(bool* buffer, int max_values) {
    max_values = std::min(max_values, num_values_);
    for (int i = 0; i < max_values; ++i) {
      if (!decoder_.Get(&buffer[i])) ParquetException::EofException();
    }
    num_values_ -= max_values;
    return max_values;
  }

 private:
  impala::RleDecoder decoder_;
};

class BoolEncoder : public Encoder {
 public:
  BoolEncoder(int buffer_size)
    : Encoder(parquet::Type::BOOLEAN, parquet::Encoding::PLAIN, buffer_size),
      encoder_(buffer_, buffer_size_, 1) {
  }

  virtual const uint8_t* Encode(int* encoded_len) {
    *encoded_len = encoder_.Flush();
    return encoder_.buffer();
  }

  virtual void Reset() {
    encoder_.Clear();
    num_values_ = 0;
  }

  virtual int Add(const bool* values, int num_values) {
    for (int i = 0; i < num_values; ++i) {
      if (!encoder_.Put(values[i])) return i;
    }
    return num_values;
  }

 private:
  impala::RleEncoder encoder_;
};

}

#endif

