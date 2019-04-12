/*
 * File Header comment
 * Copyright (c) 2019 - Your Company
 */

#include <algorithm>
#include <cassert>
#include <chrono>
#include <fstream>
#include <future>
#include <iomanip>
#include <iostream>
#include <list>
#include <sstream>
#include <string>
#include <vector>

namespace debug {

template <typename T>
bool is_sorted(T *dat, size_t sz) {
  std::vector<T> v(dat, dat + sz);
  T previous = dat[0];
  for (int i = 1; i < v.size(); ++i) {
    if (previous > dat[i]) {
      std::cerr << dat[i - 1] << " " << dat[i] << " " << dat[i + 1] << "\n";
      return false;
    }
    previous = dat[i];
  }
  return true;
}

template <typename T>
bool is_sorted(const std::string i_name) {
  const size_t mem_size = 10 * 1024 * 1024;
  const size_t mem_records_num = mem_size / sizeof(T);
  const size_t buf_size = mem_records_num * sizeof(T);

  std::ifstream is(i_name.c_str(), std::ios::in | std::ios::binary);
  if (!is.is_open()) {
    std::cerr << "Error: Cannot open file '" << i_name << "'\n";
    return false;
  }
  std::vector<T> mem(mem_records_num);
  bool done = false;
  while (!done) {
    size_t records_read = mem_records_num;
    is.read((char *)&mem[0], buf_size);
    if (!is) {
      done = true;
      records_read = is.gcount() / sizeof(T);
    }
    if (!is_sorted(&mem[0], records_read)) {
      return false;
    }
  }
  return true;
}

}  // namespace debug

std::string create_filename(const std::string &name, int n) {
  std::stringstream ss;
  ss << name << std::setfill('0') << std::setw(4) << n;
  return ss.str();
}

template <typename T>
bool sort_and_save(const std::string &file_name, T *data, size_t size) {
  std::sort(data, data + size);
  std::ofstream os(file_name.c_str(), std::ios::out | std::ios::binary);
  if (!os.is_open()) {
    std::cerr << "Error: Failed to create file " << file_name << "\n";
    return false;
  }
  os.write((char *)&data[0], size * sizeof(T));
  if (!os) {
    std::cerr << "Error: Failed to write file " << file_name << " to disk\n";
    return false;
  }
  return true;
}

template <typename T>
bool read_buff(std::ifstream &in, T *data, size_t size, size_t &records_read,
               bool &done) {
  records_read = size;
  done = false;
  in.read((char *)data, size * sizeof(T));
  if (!in) {
    done = true;
    records_read = in.gcount() / sizeof(T);
    if (records_read * sizeof(T) != in.gcount()) {
      std::cerr << "Warning: The last entry is not complete. Input file may "
                   "be damaged\n";
      return false;
    }
  }
  return true;
}

/// splits file into sorted chunks
template <typename T>
bool split(const std::string &i_name, std::vector<std::string> &o_names,
           size_t mem_size) {
  std::ifstream in(i_name.c_str(), std::ios::in | std::ios::binary);
  if (!in.is_open()) {
    std::cerr << "Error: Cannot open file '" << i_name << "'\n";
    return false;
  }

  size_t buf_records_num = mem_size / 2 / sizeof(T);
  std::vector<T> mem(buf_records_num * 2);
  int o_file_counter = 0;

  bool done = false;
  while (!done) {
    size_t records_read1;
    if (!read_buff(in, &mem[0], buf_records_num, records_read1, done)) {
      return false;
    }
    if (done) {
      std::string o_name = create_filename("tmp_split_file_", o_file_counter++);
      if (!sort_and_save(o_name, &mem[0], records_read1)) {
        std::cerr << "Error: Failed to write file " << o_name << " to disk\n";
        return false;
      }
      o_names.push_back(o_name);
    } else {
      size_t records_read2;
      if (!read_buff(in, &mem[buf_records_num], buf_records_num, records_read2,
                     done)) {
        return false;
      }
      std::string o_name1 =
          create_filename("tmp_split_file_", o_file_counter++);
      std::string o_name2 =
          create_filename("tmp_split_file_", o_file_counter++);

      // two threads ought to be enough for anybody.©
      auto ok1 = std::async(std::launch::async, sort_and_save<T>, o_name1,
                            &mem[0], records_read1);

      auto ok2 = std::async(std::launch::async, sort_and_save<T>, o_name2,
                            &mem[buf_records_num], records_read2);
      if (!ok1.get()) {
        std::cerr << "Error: Failed to write file " << o_name1 << " to disk\n";
        return false;
      }
      if (!ok2.get()) {
        std::cerr << "Error: Failed to write file " << o_name2 << " to disk\n";
        return false;
      }
      o_names.push_back(o_name1);
      o_names.push_back(o_name2);
    }
  }
  return true;
}

template <typename T>
class read_buffer {
 public:
  read_buffer(const std::string &file_name, T *buf, size_t size)
      : f_name(file_name), b(buf), sz(0), capacity(size), done(false) {
    is.open(file_name.c_str(), std::ios::in | std::ios::binary);
  }
  ~read_buffer() {
    is.close();
    remove(f_name.c_str());
  }

  bool is_open() const { return is.is_open(); }

  T *data() const { return b; }

  size_t size() const { return sz; }

  void read_next() {
    if (!is) {
      done = true;
      sz = 0;
    }
    is.read((char *)b, capacity * sizeof(T));
    sz = capacity;
    if (!is) {
      sz = is.gcount() / sizeof(T);
    }
  }

  bool depleted() const { return done; }

 private:
  std::string f_name;
  T *b;
  size_t sz;
  size_t capacity;
  std::ifstream is;
  bool done;
};

template <typename T>
class write_buffer {
 public:
  write_buffer(const std::string &file_name, T *buf, size_t size)
      : f_name(file_name), b(buf), sz(0), capacity(size) {
    os.open(file_name.c_str(), std::ios::out | std::ios::binary);
  }

  ~write_buffer() { flush(); }

  bool is_open() const { return os.is_open(); }

  void store(T data) {
    b[sz++] = data;
    if (sz == capacity) {
      flush();
    }
  }

  void store(T *data, size_t size) {
    flush();
    os.write((char *)data, size * sizeof(T));
  }

 private:
  void flush() {
    os.write((char *)b, sz * sizeof(T));
    if (!os) {
      std::cerr << "Error: Failed to write file " << f_name << " to disk\n";
    }
    sz = 0;
  }

  std::string f_name;
  T *b;
  size_t sz;
  size_t capacity;
  std::ofstream os;
};

template <typename T>
void merge_chunks(read_buffer<T> &lhs, read_buffer<T> &rhs,
                  write_buffer<T> &res) {
  lhs.read_next();
  rhs.read_next();
  T *l = lhs.data();
  size_t lsz = lhs.size();
  T *r = rhs.data();
  size_t rsz = rhs.size();

  while (!lhs.depleted() && !rhs.depleted()) {
    while (lsz && rsz) {
      if (*l < *r) {
        res.store(*l++);
        --lsz;
      } else {
        res.store(*r++);
        --rsz;
      }
    }
    if (!lsz) {
      lhs.read_next();
      l = lhs.data();
      lsz = lhs.size();
    }
    if (!rsz) {
      rhs.read_next();
      r = rhs.data();
      rsz = rhs.size();
    }
  }

  if (rsz) {
    res.store(r, rsz);
  }

  if (lsz) {
    res.store(l, lsz);
  }

  read_buffer<T> *rest = !lhs.depleted() ? &lhs : (!rhs.depleted() ? &rhs : 0);
  if (rest) {
    while (!rest->depleted()) {
      rest->read_next();
      res.store(rest->data(), rest->size());
    }
  }
}

template <typename T>
bool merge(const std::vector<std::string> &i_names, const std::string &o_name,
           size_t mem_size, int o_file_counter) {
  std::list<std::string> chunk_names(i_names.begin(), i_names.end());
  size_t mem_records_num = mem_size / sizeof(T);
  std::vector<T> mem(mem_records_num);
  size_t read_buf_size = mem_records_num / 4;
  size_t write_buf_size = read_buf_size * 2;

  while (chunk_names.size() > 1) {
    read_buffer<T> lhs(chunk_names.front(), &mem[0], read_buf_size);
    chunk_names.pop_front();
    if (!lhs.is_open()) {
      std::cerr << "Error: Cannot open file '" << chunk_names.front() << "'\n";
      return false;
    }

    read_buffer<T> rhs(chunk_names.front(), &mem[read_buf_size], read_buf_size);
    chunk_names.pop_front();
    if (!rhs.is_open()) {
      std::cerr << "Error: Cannot open file '" << chunk_names.front() << "'\n";
      return false;
    }

    std::string res_chunk_name =
        create_filename("tmp_merge_file_", o_file_counter++);
    write_buffer<T> res(res_chunk_name, &mem[read_buf_size + read_buf_size],
                        write_buf_size);
    chunk_names.push_back(res_chunk_name);
    if (!res.is_open()) {
      std::cerr << "Error: Cannot open file '" << res_chunk_name << "'\n";
      return false;
    }
    merge_chunks(lhs, rhs, res);  // TODO: std::async()
  }

  if (std::rename(chunk_names.front().c_str(), o_name.c_str())) {
    std::cerr << "Error: Failed to rename temp file to " << o_name;
    return false;
  }
  return true;
}

template <typename T>
bool sort(const std::string &i_name, const std::string &o_name,
          size_t mem_size) {
  if (mem_size / sizeof(T) < 8) {
    std::cerr << "Error: mem_size is too small\n";
    return false;
  }
  std::vector<std::string> chunk_names;
  if (!split<T>(i_name, chunk_names, mem_size)) {
    return false;
  }
  return merge<T>(chunk_names, o_name, mem_size, 0);
}

using namespace std::chrono;

struct params {
  std::string i_name = "input";
  std::string o_name = "output";
  bool do_check = false;
  bool show_usage = false;
  size_t mem_size = 120 * 1024 * 1024;

  bool parse(int argc, char **argv) {
    bool has_i = false;
    bool has_o = false;
    for (int i = 1; i < argc; i++) {
      std::string param(argv[i]);
      if (param == "--check") {
        do_check = true;
      } else if (param == "--help") {
        show_usage = true;
      } else if (param == "--mem_size") {
        if (i + 1 >= argc) {
          return false;
        }
        mem_size = std::stoi(argv[++i]);
      } else if (!has_i) {
        i_name = param;
        has_i = true;
      } else if (!has_o) {
        o_name = param;
        has_o = true;
      }
    }
    return true;
  }
};

int main(int argc, char *argv[]) {
  params p;
  if (!p.parse(argc, argv) || p.show_usage) {
    std::string argv0(argv[0]);
    std::string prog_name = argv0.substr(argv0.find_last_of("/\\") + 1);
    std::cout << "Usage: " << prog_name
              << " [OPTION]... [SOURCE] [DEST]\n"
                 "Sorts the SOURCE and writes the result to DEST\n"
                 "default SOURCE is 'input', default DEST is 'output' \n"
                 "Options:\n"
                 "  --check            check if result is sorted\n"
                 "  --mem_size SIZE    set used memory size to SIZE bytes\n"
                 "                     default is 120 MiB\n"
                 "  --help             show this message\n";
    return 0;
  }

  auto start = high_resolution_clock::now();
  if (!sort<uint32_t>(p.i_name, p.o_name, p.mem_size)) {
    return -1;
  }

  auto stop = high_resolution_clock::now();
  std::cout << "Completed in "
            << duration_cast<milliseconds>(stop - start).count() << "ms\n";

  if (p.do_check) {
    auto ok = debug::is_sorted<uint32_t>(p.o_name) ? "" : "NOT ";
    std::cout << "Check result: file '" << p.o_name << "' is " << ok
              << "sorted\n";
  }

  return 0;
}
