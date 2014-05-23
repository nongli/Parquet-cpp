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

#ifndef PARQUET_PLAIN_ENCODING_H
#define PARQUET_PLAIN_ENCODING_H

#include "encodings.h"

namespace parquet_cpp {

class PlainDecoder : public Decoder {
 public:
  PlainDecoder(const parquet::Type::type& type)
    : Decoder(type, parquet::Encoding::PLAIN), data_(NULL), len_(0) {
  }

  virtual void SetData(int num_values, const uint8_t* data, int len) {
    num_values_ = num_values;
    data_ = data;
    len_ = len;
  }

  virtual int Get(int32_t* buffer, int max_values) {
    return GetValues(buffer, max_values, sizeof(int32_t));
  }
  virtual int Get(int64_t* buffer, int max_values) {
    return GetValues(buffer, max_values, sizeof(int64_t));
  }
  virtual int Get(float* buffer, int max_values) {
    return GetValues(buffer, max_values, sizeof(float));
  }
  virtual int Get(double* buffer, int max_values) {
    return GetValues(buffer, max_values, sizeof(double));
  }
  virtual int Get(ByteArray* buffer, int max_values) {
    max_values = std::min(max_values, num_values_);
    int i = 0;
    for (; i < max_values; ++i) {
      if (len_ == 0) break;
      buffer[i].len = *reinterpret_cast<const uint32_t*>(data_);
      if (len_ < sizeof(uint32_t) + buffer[i].len) ParquetException::EofException();
      buffer[i].ptr = data_ + sizeof(uint32_t);
      data_ += sizeof(uint32_t) + buffer[i].len;
      len_ -= sizeof(uint32_t) + buffer[i].len;
    }
    num_values_ -= i;
    return i;
  }

 private:
  const uint8_t* data_;
  int len_;

  int GetValues(void* buffer, int max_values, int byte_size) {
    max_values = std::min(max_values, num_values_);
    int size = max_values * byte_size;
    if (len_ < size) {
      max_values = len_ / byte_size;
      size = max_values * byte_size;
    }
    memcpy(buffer, data_, size);
    data_ += size;
    len_ -= size;
    num_values_ -= max_values;
    return max_values;
  }
};

class PlainEncoder : public Encoder {
 public:
  PlainEncoder(const parquet::Type::type& type, int buffer_size)
    : Encoder(type, parquet::Encoding::PLAIN, buffer_size),
      offset_(0) {
    switch (type) {
      case parquet::Type::BOOLEAN:
        throw ParquetException("Boolean cannot be plain encoded.");
      case parquet::Type::INT32:
        value_size_ = sizeof(int32_t);
        break;
      case parquet::Type::INT64:
        value_size_ = sizeof(int64_t);
        break;
      case parquet::Type::FLOAT:
        value_size_ = sizeof(float);
        break;
      case parquet::Type::DOUBLE:
        value_size_ = sizeof(double);
        break;
      case parquet::Type::BYTE_ARRAY:
        value_size_ = -1;
        break;
      default:
        PARQUET_NOT_YET_IMPLEMENTED("Plain encoder");
    }
    max_values_ = buffer_size / value_size_;
  }

  virtual int Add(const int32_t* values, int num_values) {
    if (type_ != parquet::Type::INT32) {
      throw ParquetException("Plain encoder: type must be int32");
    }
    return AddInternal(values, num_values);
  }
  virtual int Add(const int64_t* values, int num_values) {
    if (type_ != parquet::Type::INT64) {
      throw ParquetException("Plain encoder: type must be int64");
    }
    return AddInternal(values, num_values);
  }
  virtual int Add(const float* values, int num_values) {
    if (type_ != parquet::Type::FLOAT) {
      throw ParquetException("Plain encoder: type must be float");
    }
    return AddInternal(values, num_values);
  }
  virtual int Add(const double* values, int num_values) {
    if (type_ != parquet::Type::DOUBLE) {
      throw ParquetException("Plain encoder: type must be double");
    }
    return AddInternal(values, num_values);
  }
  virtual int Add(const ByteArray* values, int num_values) {
    if (type_ != parquet::Type::BYTE_ARRAY) {
      throw ParquetException("Plain encoder: type must be double");
    }
    int i = 0;
    for (; i < num_values; ++i) {
      int s = sizeof(int32_t) + values[i].len;
      if (s > buffer_size_ - offset_) break;
      memcpy(buffer_ + offset_, &values[i].len, sizeof(int32_t));
      offset_ += sizeof(int32_t);
      memcpy(buffer_ + offset_, values[i].ptr, values[i].len);
      offset_ += values[i].len;
    }
    num_values_ += i;
    return i;
  }

  virtual const uint8_t* Encode(int* encoded_len) {
    *encoded_len = offset_;
    return buffer_;
  }

  virtual void Reset() {
    num_values_ = 0;
    offset_ = 0;
  }

 private:
  int max_values_;
  int value_size_;
  int offset_;

  int AddInternal(const void* values, int num_values) {
    int to_copy = std::min(num_values, max_values_ - num_values_);
    memcpy(buffer_ + offset_, values, to_copy * value_size_);
    num_values_ += to_copy;
    offset_ += to_copy * value_size_;
    return to_copy;
  }
};

}

#endif

