#pragma once

#include <boost/intrusive_ptr.hpp>
#include <memory>
#include <terark/fstring.hpp>
#include <terark/int_vector.hpp>
#include <terark/io/DataIO.hpp>
#include <terark/io/StreamBuffer.hpp>
#include <terark/util/fstrvec.hpp>
#include <terark/util/refcount.hpp>
#include <terark/valvec.hpp>

using terark::febitvec;
using terark::valvec;

namespace terark {

template <size_t P, size_t W> class rank_select_few_builder;

/* rank_select_few is a succinct rank select structure specialized for some 
 * cases that one kind of element is far lesser than the other. In this case,
 * we call minor element as pivot, other element as complement, which better
 * illustrating states of elements.
 * 
 * The following example is showing a case that "1" is pivot. 
 * --------------------------------------------------------------------------
 * position |  0 |  1 |  2 |  3 |  4 |  5 |  6 |  7 |  8 |  9 | 10 | 11 | ...
 * --------------------------------^----------------------------------^------
 * element  |  0 |  0 |  0 |  0 |  1 |  0 |  0 |  0 |  0 |  0 |  0 |  1 | ...
 * --------------------------------^----------------------------------^------
 *     |  |        
 *    \|  |/   We can store only pivots' position by rank without losing any 
 *     \  /    infomation.
 *      \/
 * ---------------------------------------------------
 * pivots' rank     : logical address |  0 |  1 | ...
 * ---------------------------------------------------
 * pivots' position : data in storage |  4 | 11 | ...
 * ---------------------------------------------------
 */
template <size_t P, size_t W> class rank_select_few {
  friend class rank_select_few_builder<P, W>;

private:

  // lower_bound in pivots
  size_t lower_bound(size_t val) const;
  size_t lower_bound(size_t val, size_t &hint) const;

  // select complement element with id, i.e. rank of complement
  size_t select_complement(size_t id) const;
  size_t select_complement(size_t id, size_t &hint) const;

  // value from logical address
  inline size_t val_a_logi(size_t pos) const {
    return (*reinterpret_cast<const size_t *>(m_mempool.data() + pos * W)) &
           (W == 8 ? size_t(-1) : (1ULL << ((W * 8) & 63)) - 1);
  }

  // value of pointer
  inline size_t val_of_ptr(const uint8_t* ptr) const {
    return (*reinterpret_cast<const size_t *>(ptr)) &
           (W == 8 ? size_t(-1) : (1ULL << ((W * 8) & 63)) - 1);
  }

public:
  rank_select_few() : m_num0(), m_num1(), m_layer(), m_offset() {}

  // return true if value at pos is 1.
  bool operator[](size_t pos) const;

  // impl of hint probing  
  bool at_with_hint(size_t pos, size_t &hint) const;

  // return true if value at pos is 0.
  bool is0(size_t pos) const { return !operator[](pos); }
  bool is0(size_t pos, size_t &hint) const { return !at_with_hint(pos, hint); }

  // return true if value at pos is 1.
  bool is1(size_t pos) const { return operator[](pos); }
  bool is1(size_t pos, size_t &hint) const { return at_with_hint(pos, hint); }

  // return element rank of pos
  size_t rank0(size_t pos) const;
  size_t rank0(size_t pos, size_t &hint) const;
  size_t rank1(size_t pos) const;
  size_t rank1(size_t pos, size_t &hint) const;

  // return pos of select element
  size_t select0(size_t id) const;
  size_t select0(size_t id, size_t &hint) const;
  size_t select1(size_t id) const;
  size_t select1(size_t id, size_t &hint) const;

  // return length of continuous 0-sequence of pos  
  size_t zero_seq_len(size_t pos) const;
  size_t zero_seq_len(size_t pos, size_t &hint) const;

  // return length of reverse continuous 0-sequence of pos  
  size_t zero_seq_revlen(size_t pos) const;
  size_t zero_seq_revlen(size_t pos, size_t &hint) const;

  // return length of continuous 1-sequence of pos  
  size_t one_seq_len(size_t pos) const;
  size_t one_seq_len(size_t pos, size_t &hint) const;

  // return length of reverse continuous 1-sequence of pos  
  size_t one_seq_revlen(size_t pos) const;
  size_t one_seq_revlen(size_t pos, size_t &hint) const;

  // return maximum of rank 0
  size_t max_rank0() const { return m_num0; }

  // return maximum of rank 1
  size_t max_rank1() const { return m_num1; }

  // return size of elements
  size_t size() const { return m_num0 + m_num1; }

  // return raw data pointer
  const byte_t *data() const { return m_mempool.data(); }

  // return structure occupied memory size 
  size_t mem_size() const { return m_mempool.full_mem_size(); }

  // swap operator
  void swap(rank_select_few &r) {
    std::swap(m_num0, r.m_num0);
    std::swap(m_num1, r.m_num1);
    std::swap(m_offset, r.m_offset);
    std::swap(m_layer, r.m_layer);
    m_mempool.swap(r.m_mempool);
  }

  // force structuralize certain memory
  void risk_mmap_from(unsigned char *src, size_t size) {
    m_mempool.risk_set_data(src, size);
    uint64_t *ptr = reinterpret_cast<uint64_t*>(m_mempool.data() + size - 8);
    m_layer = *ptr;
    assert(m_layer < 8);
    ptr -= m_layer;
    m_offset = ptr--;
    m_num1 = *ptr--;
    m_num0 = *ptr;
  }

  // release ownership without destroying internal elements
  void risk_release_ownership() { m_mempool.risk_release_ownership(); }

  void clear() { m_mempool.clear(); }

private:
  uint64_t m_num0, m_num1, m_layer;
  uint64_t *m_offset;
  valvec<uint8_t> m_mempool;
};

/*
 * Tool class to build rank_select_few.
 * init this builder with number of zeroes and ones, reverse build or not
 * For example, to build follow structrue.
 * ----------------------------------------------------------------------
 * position |  0 |  1 |  2 |  3 |  4 |  5 |  6 |  7 |  8 |  9 | 10 | 11 |
 * --------------------------------^----------------------------------^--
 * element  |  0 |  0 |  0 |  0 |  1 |  0 |  0 |  0 |  0 |  0 |  0 |  1 |
 * --------------------------------^----------------------------------^--
 * {
 *   rank_select_few<1, 4> rs;
 *   rank_select_few_builder<1, 4> rs_builder(10, 2, false);
 *   rs_builder.insert(4);
 *   rs_builder.insert(11);
 *   rs_builder.finish(&rs);
 * }
 * in case 3rd argument is true, insertion should be descending.
 */
template <size_t P, size_t W> class rank_select_few_builder {
  friend class rank_select_few<P, W>;

public:
  // construct rs_builder with number of elements and insert direction
  rank_select_few_builder(size_t num0, size_t num1, bool rev);
  ~rank_select_few_builder() {}

  // insert ones position
  void insert(size_t pos);

  // finish building process and transport to certain rank_select_few class
  void finish(rank_select_few<P, W> *);

private:
  bool m_rev;
  uint64_t m_last;
  uint8_t *m_it;
  uint64_t m_num0, m_num1, m_layer;
  uint64_t *m_offset;
  valvec<uint8_t> m_mempool;
};

template <size_t W> using rank_select_fewzero = rank_select_few<0, W>;
template <size_t W> using rank_select_fewone = rank_select_few<1, W>;

// rank_select_few wrapper for compatibility
template<class T>
class rank_select_hint_wrapper {
  const T& rs_;
public:
  rank_select_hint_wrapper(const T& rs, size_t* /*hint*/) : rs_(rs) {}

  bool operator[](size_t pos) const { return rs_.is1(pos); }
  bool is0(size_t pos) const { return rs_.is0(pos); }
  bool is1(size_t pos) const { return rs_.is1(pos); }
  size_t rank0(size_t pos) const { return rs_.rank0(pos); }
  size_t rank1(size_t pos) const { return rs_.rank1(pos); }
  size_t select0(size_t pos) const { return rs_.select0(pos); }
  size_t select1(size_t pos) const { return rs_.select1(pos); }
  size_t zero_seq_len(size_t pos) const { return rs_.zero_seq_len(pos); }
  size_t zero_seq_revlen(size_t pos) const { return rs_.zero_seq_revlen(pos); }
  size_t one_seq_len(size_t pos) const { return rs_.one_seq_len(pos); }
  size_t one_seq_revlen(size_t pos) const { return rs_.one_seq_revlen(pos); }

  size_t max_rank0() const { return rs_.max_rank0(); }
  size_t max_rank1() const { return rs_.max_rank1(); }
  size_t size() const { return rs_.size(); }
};

// rank_select_few with hint wrapper for compatibility
template<size_t P, size_t W>
class rank_select_hint_wrapper<rank_select_few<P, W>> {
  const rank_select_few<P, W>& rs_;
  size_t* h_;
public:
  rank_select_hint_wrapper(const rank_select_few<P, W>& rs, size_t* hint) : rs_(rs), h_(hint) {
    assert(hint != nullptr);
  }

  bool operator[](size_t pos) const { return rs_.is1(pos, *h_); }
  bool is0(size_t pos) const { return rs_.is0(pos, *h_); }
  bool is1(size_t pos) const { return rs_.is1(pos, *h_); }
  size_t rank0(size_t pos) const { return rs_.rank0(pos, *h_); }
  size_t rank1(size_t pos) const { return rs_.rank1(pos, *h_); }
  size_t select0(size_t pos) const { return rs_.select0(pos, *h_); }
  size_t select1(size_t pos) const { return rs_.select1(pos, *h_); }
  size_t zero_seq_len(size_t pos) const { return rs_.zero_seq_len(pos, *h_); }
  size_t zero_seq_revlen(size_t pos) const { return rs_.zero_seq_revlen(pos, *h_); }
  size_t one_seq_len(size_t pos) const { return rs_.one_seq_len(pos, *h_); }
  size_t one_seq_revlen(size_t pos) const { return rs_.one_seq_revlen(pos, *h_); }

  size_t max_rank0() const { return rs_.max_rank0(); }
  size_t max_rank1() const { return rs_.max_rank1(); }
  size_t size() const { return rs_.size(); }
};

template<class T>
rank_select_hint_wrapper<T> make_rank_select_hint_wrapper(const T& rs, size_t* hint) {
  return rank_select_hint_wrapper<T>(rs, hint);
}

} // namespace terark
