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

#ifndef PARQUET_DELTA_BYTE_ARRAY_ENCODING_H
#define PARQUET_DELTA_BYTE_ARRAY_ENCODING_H

#include "encodings.h"

namespace parquet_cpp {

class DeltaByteArrayDecoder : public Decoder {
 public:
  DeltaByteArrayDecoder()
    : Decoder(parquet::Type::BYTE_ARRAY, parquet::Encoding::DELTA_BYTE_ARRAY),
      prefix_len_decoder_(parquet::Type::INT32),
      suffix_decoder_() {
  }

  virtual void SetData(int num_values, const uint8_t* data, int len) {
    num_values_ = num_values;
    if (len == 0) return;
    int prefix_len_length = *reinterpret_cast<const int*>(data);
    data += 4;
    len -= 4;
    prefix_len_decoder_.SetData(num_values, data, prefix_len_length);
    data += prefix_len_length;
    len -= prefix_len_length;
    suffix_decoder_.SetData(num_values, data, len);
  }

  // TODO: this doesn't work and requires memory management. We need to allocate
  // new strings to store the results.
  virtual int Get(ByteArray* buffer, int max_values) {
    max_values = std::min(max_values, num_values_);
    for (int  i = 0; i < max_values; ++i) {
      int prefix_len = 0;
      prefix_len_decoder_.Get(&prefix_len, 1);
      ByteArray suffix;
      suffix_decoder_.Get(&suffix, 1);
      buffer[i].len = prefix_len + suffix.len;

      uint8_t* result = reinterpret_cast<uint8_t*>(malloc(buffer[i].len));
      memcpy(result, last_value_.ptr, prefix_len);
      memcpy(result + prefix_len, suffix.ptr, suffix.len);

      buffer[i].ptr = result;
      last_value_ = buffer[i];
    }
    num_values_ -= max_values;
    return max_values;
  }

 private:
  DeltaBitPackDecoder prefix_len_decoder_;
  DeltaLengthByteArrayDecoder suffix_decoder_;
  ByteArray last_value_;
};

class DeltaByteArrayEncoder : public Encoder {
 public:
  DeltaByteArrayEncoder(int buffer_size)
    : Encoder(parquet::Type::BYTE_ARRAY, parquet::Encoding::DELTA_BYTE_ARRAY,
        buffer_size),
      prefix_len_encoder_(parquet::Type::INT32, buffer_size),
      suffix_encoder_(buffer_size),
      plain_encoded_len_(0) {
  }

  void AddValue(const std::string& s) {
    return AddValue(reinterpret_cast<const uint8_t*>(s.data()), s.size());
  }

  void AddValue(const uint8_t* ptr, int len) {
    plain_encoded_len_ += len + sizeof(int);
    int min_len = std::min(len, static_cast<int>(last_value_.size()));
    int prefix_len = 0;
    for (int i = 0; i < min_len; ++i) {
      if (ptr[i] == last_value_[i]) {
        ++prefix_len;
      } else {
        break;
      }
    }
    prefix_len_encoder_.Add(&prefix_len, 1);
    suffix_encoder_.AddValue(ptr + prefix_len, len - prefix_len);
    last_value_ = std::string(reinterpret_cast<const char*>(ptr), len);
    ++num_values_;
  }

  virtual int Add(const ByteArray* values, int num_values) {
    if (type_ != parquet::Type::BYTE_ARRAY) {
      throw ParquetException("DeltaByteArrayEncoder encoder: type must be byte array");
    }
    for (int i = 0; i < num_values; ++i) {
      AddValue(values[i].ptr, values[i].len);
    }
    num_values_ += num_values;
    return num_values;
  }

  virtual const uint8_t* Encode(int* encoded_len) {
    int prefix_buffer_len;
    const uint8_t* prefix_buffer = prefix_len_encoder_.Encode(&prefix_buffer_len);
    int suffix_buffer_len;
    const uint8_t* suffix_buffer = suffix_encoder_.Encode(&suffix_buffer_len);

    // TODO: not right. Need memory management.
    uint8_t* buffer = new uint8_t[10 * 1024 * 1024];
    memcpy(buffer, &prefix_buffer_len, sizeof(int));
    memcpy(buffer + sizeof(int), prefix_buffer, prefix_buffer_len);
    memcpy(buffer + sizeof(int) + prefix_buffer_len, suffix_buffer, suffix_buffer_len);
    *encoded_len = sizeof(int) + prefix_buffer_len + suffix_buffer_len;
    return buffer;
  }

  virtual void Reset() {
    prefix_len_encoder_.Reset();
    suffix_encoder_.Reset();
    last_value_ = "";
    plain_encoded_len_ = 0;
  }

  int plain_encoded_len() const { return plain_encoded_len_; }

 private:
  DeltaBitPackEncoder prefix_len_encoder_;
  DeltaLengthByteArrayEncoder suffix_encoder_;
  std::string last_value_;
  int plain_encoded_len_;
};

}

#endif

