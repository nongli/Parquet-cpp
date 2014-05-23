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

#ifndef PARQUET_DELTA_BIT_PACK_ENCODING_H
#define PARQUET_DELTA_BIT_PACK_ENCODING_H

#include "encodings.h"

namespace parquet_cpp {

class DeltaBitPackDecoder : public Decoder {
 public:
  DeltaBitPackDecoder(const parquet::Type::type& type)
    : Decoder(type, parquet::Encoding::DELTA_BINARY_PACKED) {
    if (type != parquet::Type::INT32 && type != parquet::Type::INT64) {
      throw ParquetException("Delta bit pack encoding should only be for integer data.");
    }
  }

  virtual void SetData(int num_values, const uint8_t* data, int len) {
    num_values_ = num_values;
    decoder_ = impala::BitReader(data, len);
    values_current_block_ = 0;
    values_current_mini_block_ = 0;
  }

  virtual int Get(int32_t* buffer, int max_values) {
    return GetInternal(buffer, max_values);
  }

  virtual int Get(int64_t* buffer, int max_values) {
    return GetInternal(buffer, max_values);
  }

 private:
  void InitBlock() {
    uint64_t block_size;
    if (!decoder_.GetVlqInt(&block_size)) ParquetException::EofException();
    if (!decoder_.GetVlqInt(&num_mini_blocks_)) ParquetException::EofException();
    if (!decoder_.GetVlqInt(&values_current_block_)) {
      ParquetException::EofException();
    }
    if (!decoder_.GetZigZagVlqInt(&last_value_)) ParquetException::EofException();
    delta_bit_widths_.resize(num_mini_blocks_);

    if (!decoder_.GetZigZagVlqInt(&min_delta_)) ParquetException::EofException();
    for (int i = 0; i < num_mini_blocks_; ++i) {
      if (!decoder_.GetAligned<uint8_t>(1, &delta_bit_widths_[i])) {
        ParquetException::EofException();
      }
    }
    values_per_mini_block_ = block_size / num_mini_blocks_;
    mini_block_idx_ = 0;
    delta_bit_width_ = delta_bit_widths_[0];
    values_current_mini_block_ = values_per_mini_block_;
  }

  template <typename T>
  int GetInternal(T* buffer, int max_values) {
    max_values = std::min(max_values, num_values_);
    for (int i = 0; i < max_values; ++i) {
      if (UNLIKELY(values_current_mini_block_ == 0)) {
        ++mini_block_idx_;
        if (mini_block_idx_ < delta_bit_widths_.size()) {
          delta_bit_width_ = delta_bit_widths_[mini_block_idx_];
          values_current_mini_block_ = values_per_mini_block_;
        } else {
          InitBlock();
          buffer[i] = last_value_;
          continue;
        }
      }

      // TODO: the key to this algorithm is to decode the entire miniblock at once.
      int64_t delta;
      if (!decoder_.GetValue(delta_bit_width_, &delta)) ParquetException::EofException();
      delta += min_delta_;
      last_value_ += delta;
      buffer[i] = last_value_;
      --values_current_mini_block_;
    }
    num_values_ -= max_values;
    return max_values;
  }

  impala::BitReader decoder_;
  uint64_t values_current_block_;
  uint64_t num_mini_blocks_;
  uint64_t values_per_mini_block_;
  uint64_t values_current_mini_block_;

  int64_t min_delta_;
  int mini_block_idx_;
  std::vector<uint8_t> delta_bit_widths_;
  int delta_bit_width_;

  int64_t last_value_;
};

class DeltaBitPackEncoder : public Encoder {
 public:
  DeltaBitPackEncoder(const parquet::Type::type& type, int buffer_size,
      int mini_block_size = 8)
    : Encoder(type, parquet::Encoding::DELTA_BINARY_PACKED, buffer_size),
      mini_block_size_(mini_block_size) {
    switch (type) {
      case parquet::Type::INT32:
      case parquet::Type::INT64:
        break;
      default:
        throw ParquetException("Only int types are valid.");
    }
  }

  virtual void Reset() {
    values_.clear();
    num_values_ = 0;
  }

  virtual int Add(const int32_t* values, int num_values) {
    for (int i = 0; i < num_values; ++i) {
      values_.push_back(values[i]);
    }
    num_values_ += num_values;
    return num_values;
  }
  virtual int Add(const int64_t* values, int num_values) {
    for (int i = 0; i < num_values; ++i) {
      values_.push_back(values[i]);
    }
    num_values_ += num_values;
    return num_values;
  }

  virtual const uint8_t* Encode(int* encoded_len) {
    // TODO: not right, error handling
    uint8_t* result = new uint8_t[10 * 1024 * 1024];
    int num_mini_blocks = impala::BitUtil::Ceil(num_values() - 1, mini_block_size_);
    uint8_t* mini_block_widths = NULL;

    impala::BitWriter writer(result, 10 * 1024 * 1024);

    // Writer the size of each block. We only use 1 block currently.
    writer.PutVlqInt(num_mini_blocks * mini_block_size_);

    // Write the number of mini blocks.
    writer.PutVlqInt(num_mini_blocks);

    // Write the number of values.
    writer.PutVlqInt(num_values() - 1);

    // Write the first value.
    writer.PutZigZagVlqInt(values_[0]);

    // Compute the values as deltas and the min delta.
    int64_t min_delta = std::numeric_limits<int64_t>::max();
    for (int i = values_.size() - 1; i > 0; --i) {
      values_[i] -= values_[i - 1];
      min_delta = std::min(min_delta, values_[i]);
    }

    // Write out the min delta.
    writer.PutZigZagVlqInt(min_delta);

    // We need to save num_mini_blocks bytes to store the bit widths of the mini blocks.
    mini_block_widths = writer.GetNextBytePtr(num_mini_blocks);

    int idx = 1;
    for (int i = 0; i < num_mini_blocks; ++i) {
      int n = std::min(mini_block_size_, num_values() - idx);

      // Compute the max delta in this mini block.
      int64_t max_delta = std::numeric_limits<int64_t>::min();
      for (int j = 0; j < n; ++j) {
        max_delta = std::max(values_[idx + j], max_delta);
      }

      // The bit width for this block is the number of bits needed to store
      // (max_delta - min_delta).
      int bit_width = impala::BitUtil::NumRequiredBits(max_delta - min_delta);
      mini_block_widths[i] = bit_width;

      // Encode this mini blocking using min_delta and bit_width
      for (int j = 0; j < n; ++j) {
        writer.PutValue(values_[idx + j] - min_delta, bit_width);
      }

      // Pad out the last block.
      for (int j = n; j < mini_block_size_; ++j) {
        writer.PutValue(0, bit_width);
      }
      idx += n;
    }

    writer.Flush();
    *encoded_len = writer.bytes_written();
    return result;
  }

 private:
  const int mini_block_size_;
  std::vector<int64_t> values_;
};

}

#endif

