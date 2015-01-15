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

#ifndef PARQUET_SCHEMA_H
#define PARQUET_SCHEMA_H

#include <exception>
#include <sstream>
#include <boost/cstdint.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/unordered_map.hpp>
#include "gen-cpp/parquet_constants.h"
#include "gen-cpp/parquet_types.h"

namespace parquet_cpp {

// Object that stores a projection (e.g. a subset of the columns).
// TODO: this is not sufficient to do schema resolution. This should
// be augmented to include types, default values, etc.
class Projection {
 public:
  // Creates a projection from a list of paths.
  // e.g. cols[0] is the path (by name).
  // Paths are case insensitive.
  Projection(const std::vector<std::vector<std::string> >& cols);

 private:
  friend class Schema;
  std::vector<std::vector<std::string> > cols_by_name_;
};

// Tree representation of a parquet schema (which is encoded as a flattened tree
// in thrift).
// Many of these APIs have a projected version. This ignores children that are
// not projected.
class Schema {
 public:
  class Element {
   public:
    const parquet::SchemaElement& parquet_schema() const { return parquet_schema_; }
    const Element* parent() const { return parent_; }
    int max_def_level() const { return max_def_level_; }
    int max_rep_level() const { return max_rep_level_; }
    bool is_root() const { return parent_ == NULL; }

    const std::string& name() const { return parquet_schema_.name; }
    const std::string& full_name() const { return full_name_; }
    const std::vector<std::string>& string_path() const { return string_path_; }
    int num_children() const { return children_.size(); }
    const Schema::Element* child(int idx) const { return children_[idx].get(); }


    int num_projected_children() const { return projected_children_.size(); }
    const Schema::Element* projected_child(int idx) const {
      return projected_children_[idx];
    }

    bool is_repeated() const {
      return parquet_schema_.repetition_type == parquet::FieldRepetitionType::REPEATED;
    }

    // Returns the ordinal of child with 'child_name'.
    // e.g. for a schema like
    // struct S {
    //   int a;
    //   int b;
    // };
    // If Element is for 'S', IndexOf('a') returns 0 and IndexOf('b') returns 1.
    // If projected_only is true, ignoring non-projected columns.
    int IndexOf(const std::string& child_name, bool projected_only) const;

    // Returns the index this elemnt is in the parent.
    // Equivalent to parent()->IndexOf(this->name())
    // Computed in Compile().
    int index_in_parent() const { return index_in_parent_; }
    int projected_index_in_parent() const { return projected_index_in_parent_; }

    // Returns the ordinal (aka index_in_parent) path up to the root, starting
    // with the root.
    // e.g. for a schema
    // struct root {
    //   struct a {}
    //   struct b {
    //     struct c {
    //       int x;
    //       int y; <-- This element maps to root.c.y
    //     }
    //   }
    // this would 1 (b's index in root), 0 (c's index in b)
    // Computed in Compile().
    const std::vector<const Element*> schema_path() const { return schema_path_; }
    const std::vector<int> ordinal_path() const { return ordinal_path_; }
    const std::vector<int> projected_ordinal_path() const {
      return projected_ordinal_path_;
    }

    std::string ToString(const std::string& prefix = "",
        bool projected_only = false) const;

   private:
    friend class Schema;

    Element(const parquet::SchemaElement& e, Element* parent);

    // Must be called after the schema is fully constructed.
    void Compile();

    // Sets projection_ to false for this node and its subtree.
    void ClearProjection();

    const parquet::SchemaElement parquet_schema_;
    const Element* parent_;
    std::vector<boost::shared_ptr<Element> > children_;
    const int max_def_level_;
    const int max_rep_level_;

    // Some precomputed data values. Set in Compile()

    // Index of this element in the parent.
    int index_in_parent_;

    // Schema objects starting from the root to this element (including this element).
    std::vector<const Element*> schema_path_;

    // Names of elements from the root.
    std::vector<std::string> string_path_;

    // Path of index_in_parent_ from root to this element.
    std::vector<int> ordinal_path_;

    // Full path ('.' separated) from root to this element (including this element).
    std::string full_name_;

    // TODO: move this state and below to another object that handles projected
    // schemas.
    bool projected_;

    // The index in the parent, ignoring siblings that are not projected.
    // e.g.
    // struct {
    //   int x;
    //   int y;
    // }
    // y always has index_in_parent_ == 1 but projected_index_in_parent_ could
    // be 0 (if x is not projected) or 1 (if x is projected).
    int projected_index_in_parent_;

    // Subset of children_ containing only projected elements.
    std::vector<Element*> projected_children_;
    std::vector<int> projected_ordinal_path_;
  };

  static boost::shared_ptr<Schema> FromParquet(
      const std::vector<parquet::SchemaElement>& root);

  // Sets the projection to use for this schema. If this is not called, all
  // columns are projected.
  // TODO: this doesn't seem like the right abstraction. Schema should be
  // const and we should have a mirroring structure for readers that takes
  // a const schema and a projection and precomputes the stuff stores in
  // here (e.g. the schema_path_, oridinal_path_, etc).
  void SetProjection(Projection* projection);

  const Element* root() { return root_.get(); }
  const std::vector<Element*> leaves() const { return leaves_; }
  const std::vector<Element*> projected_leaves() const { return projected_leaves_; }
  int max_def_level() const { return max_def_level_; }

  std::string ToString(bool projected_only = false) const {
    return root_->ToString("", projected_only);
  }

 private:
  Schema() : max_def_level_(0), projection_(NULL) {}
  Schema(const Schema&);
  Schema& operator=(const Schema&);

  boost::scoped_ptr<Element> root_;
  std::vector<Element*> leaves_;
  std::vector<Element*> projected_leaves_;
  int max_def_level_;

  Projection* projection_;

  void Parse(const std::vector<parquet::SchemaElement>& nodes,
      Element* parent, int* idx);
};

}

#endif
