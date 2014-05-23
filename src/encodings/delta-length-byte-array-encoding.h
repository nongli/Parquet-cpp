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

#ifndef PARQUET_DELTA_LENGTH_BYTE_ARRAY_ENCODING_H
#define PARQUET_DELTA_LENGTH_BYTE_ARRAY_ENCODING_H

#include "encodings.h"

namespace parquet_cpp {

class DeltaLengthByteArrayDecoder : public Decoder {
 public:
  DeltaLengthByteArrayDecoder()
    : Decoder(parquet::Type::BYTE_ARRAY, parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY),
      len_decoder_(parquet::Type::INT32) {
  }

  virtual void SetData(int num_values, const uint8_t* data, int len) {
    num_values_ = num_values;
    if (len == 0) return;
    int total_lengths_len = *reinterpret_cast<const int*>(data);
    data += 4;
    len_decoder_.SetData(num_values, data, total_lengths_len);
    data_ = data + total_lengths_len;
    len_ = len - 4 - total_lengths_len;
  }

  virtual int Get(ByteArray* buffer, int max_values) {
    max_values = std::min(max_values, num_values_);
    int lengths[max_values];
    len_decoder_.Get(lengths, max_values);
    for (int  i = 0; i < max_values; ++i) {
      buffer[i].len = lengths[i];
      buffer[i].ptr = data_;
      data_ += lengths[i];
      len_ -= lengths[i];
    }
    num_values_ -= max_values;
    return max_values;
  }

 private:
  DeltaBitPackDecoder len_decoder_;
  const uint8_t* data_;
  int len_;
};

class DeltaLengthByteArrayEncoder : public Encoder {
 public:
  DeltaLengthByteArrayEncoder(int buffer_size, int mini_block_size = 8)
    : Encoder(parquet::Type::BYTE_ARRAY, parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY,
        buffer_size),
      len_encoder_(parquet::Type::INT32, 1, mini_block_size),
      offset_(0),
      plain_encoded_len_(0) {
  }

  void AddValue(const std::string& s) {
    AddValue(reinterpret_cast<const uint8_t*>(s.data()), s.size());
  }

  void AddValue(const uint8_t* ptr, int len) {
    plain_encoded_len_ += len + sizeof(int);
    len_encoder_.Add(&len, 1);
    memcpy(buffer_ + offset_, ptr, len);
    offset_ += len;
    ++num_values_;
  }

  virtual int Add(const ByteArray* values, int num_values) {
    if (type_ != parquet::Type::BYTE_ARRAY) {
      throw ParquetException("DeltaByteArrayEncoder encoder: type must be byte array");
    }
    for (int i = 0; i < num_values; ++i) {
      AddValue(values[i].ptr, values[i].len);
    }
    return num_values;
  }

  virtual const uint8_t* Encode(int* encoded_len) {
    // TODO: not right. Need memory management.
    const uint8_t* encoded_lengths = len_encoder_.Encode(encoded_len);
    memmove(buffer_ + *encoded_len + sizeof(int), buffer_, offset_);
    memcpy(buffer_, encoded_len, sizeof(int));
    memcpy(buffer_ + sizeof(int), encoded_lengths, *encoded_len);
    *encoded_len += offset_ + sizeof(int);
    return buffer_;
  }

  virtual void Reset() {
    len_encoder_.Reset();
    offset_ = 0;
    plain_encoded_len_ = 0;
  }

  int plain_encoded_len() const { return plain_encoded_len_; }

 private:
  DeltaBitPackEncoder len_encoder_;
  int offset_;
  int plain_encoded_len_;
};

}

#endif

