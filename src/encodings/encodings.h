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

#ifndef PARQUET_ENCODINGS_H
#define PARQUET_ENCODINGS_H

#include <boost/cstdint.hpp>
#include "gen-cpp/parquet_constants.h"
#include "gen-cpp/parquet_types.h"

#include "impala/rle-encoding.h"
#include "impala/bit-stream-utils.inline.h"

namespace parquet_cpp {

// Base class for all decoders.
class Decoder {
 public:
  virtual ~Decoder() {}

  // Sets the data for a new page. This will be called multiple times on the same
  // decoder and should reset all internal state.
  virtual void SetData(int num_values, const uint8_t* data, int len) = 0;

  // Subclasses should override the ones they support. In each of these functions,
  // the decoder would decode put to 'max_values', storing the result in 'buffer'.
  // The function returns the number of values decoded, which should be max_values
  // except for end of the current data page.
  virtual int Get(bool* buffer, int max_values) {
    throw ParquetException("Decoder does not implement this type.");
  }
  virtual int Get(int32_t* buffer, int max_values) {
    throw ParquetException("Decoder does not implement this type.");
  }
  virtual int Get(int64_t* buffer, int max_values) {
    throw ParquetException("Decoder does not implement this type.");
  }
  virtual int Get(float* buffer, int max_values) {
    throw ParquetException("Decoder does not implement this type.");
  }
  virtual int Get(double* buffer, int max_values) {
    throw ParquetException("Decoder does not implement this type.");
  }
  virtual int Get(ByteArray* buffer, int max_values) {
    throw ParquetException("Decoder does not implement this type.");
  }

  // Returns the number of values left (for the last call to SetData()). This is
  // the number of values left in this page.
  int values_left() const { return num_values_; }

  const parquet::Encoding::type encoding() const { return encoding_; }

 protected:
  Decoder(const parquet::Type::type& type, const parquet::Encoding::type& encoding)
    : type_(type), encoding_(encoding), num_values_(0) {}

  const parquet::Type::type type_;
  const parquet::Encoding::type encoding_;
  int num_values_;
};

// Base class for all encoders.
// The expected calling pattern is:
// Encoder e;
// while (!eos) {
//   e.Add();
//   e.Add();...
//   buffer = e.Encode()
//   e.Reset()
// }
class Encoder {
 public:
  virtual ~Encoder() {
    delete[] buffer_;
  }

  // Returns the encoded data for all values called to Add*() after the last
  // Reset(). Conceptually, Add*() buffers the values and generates the encoded
  // result on Encode(). Repeated calls to Encode() (with no other calls in
  // between), will return the same thing.
  // The returned buffer is owned by the Encoder() and valid until the next
  // call to Encode() or Reset().
  virtual const uint8_t* Encode(int* encoded_len) = 0;

  // Resets the encoder state.
  virtual void Reset() = 0;

  // Adds 'num_values' to the encoder. The encoder is responsible for copying the
  // values if it needs to, e.g. 'values' can be reused/freed by the caller.
  // returns the number of values encoded. If this is less than num_values, it
  // indicates there was not enough memory to buffer/encode the values. The
  // caller should call Encode()/Reset() before calling Add*() again.
  // Subclasses should implement the types they support.
  virtual int Add(const bool* values, int num_values) {
    throw ParquetException("Encoder does not implement this type.");
  }
  virtual int Add(const int32_t* values, int num_values) {
    throw ParquetException("Encoder does not implement this type.");
  }
  virtual int Add(const int64_t* values, int num_values) {
    throw ParquetException("Encoder does not implement this type.");
  }
  virtual int Add(const float* values, int num_values) {
    throw ParquetException("Encoder does not implement this type.");
  }
  virtual int Add(const double* values, int num_values) {
    throw ParquetException("Encoder does not implement this type.");
  }
  virtual int Add(const ByteArray* values, int num_values) {
    throw ParquetException("Encoder does not implement this type.");
  }

  // TODO:
  // Functions for estimated/min/max encoded size?

  // Returns the number of values added since the last call to Reset().
  int num_values() const { return num_values_; }

  const parquet::Type::type type() const { return type_; }
  const parquet::Encoding::type encoding() const { return encoding_; }

 protected:
  Encoder(const parquet::Type::type type, const parquet::Encoding::type& encoding,
      int buffer_size)
    : type_(type),
      encoding_(encoding),
      buffer_size_(buffer_size),
      buffer_(new uint8_t[buffer_size_]),
      num_values_(0) {
  }

  const parquet::Type::type type_;
  const parquet::Encoding::type encoding_;
  const int buffer_size_;
  uint8_t* buffer_;
  int num_values_;
};

}

#include "bool-encoding.h"
#include "plain-encoding.h"
#include "dictionary-encoding.h"
#include "delta-bit-pack-encoding.h"
#include "delta-length-byte-array-encoding.h"
#include "delta-byte-array-encoding.h"

#endif

