// find top K frequent qq numbers in 1000000 qq numbers

#include <algorithm>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <iostream>
#include <memory>
#include <queue>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

inline clock_t time_begin() { return std::clock(); }

template <typename... Vars>
inline void time_end(const clock_t begin, const char* message, Vars&&... args) {
  printf("Task: ");
  printf(message, args...);
  printf("\t--%.2fs\n", double(std::clock() - begin) / CLOCKS_PER_SEC);
}

template <>
inline void time_end(const clock_t begin, const char* message) {
  time_end(begin, "%s", message);
}

void create_random_qq_datafile(const char* file, const int n,
                               const int max_qq_number) {
  auto begin = time_begin();
  const int kThreadCount = 2;
  std::ofstream out(file);
  std::vector<std::thread> threads;
  for (int j = 0; j < kThreadCount; j++) {
    threads.emplace_back([&]() {
      int index = j;
      char buffer[16];
      std::random_device rd;
      std::mt19937 gen(rd());
      std::uniform_int_distribution<> dis(1, max_qq_number);
      for (int i = 0; i < n / kThreadCount; i++) {
        snprintf(buffer, 16, "%d\n", dis(gen));
        out << buffer;
      }
      if (index == j - 1) {
        for (int i = 0; i < n % kThreadCount; i++) {
          snprintf(buffer, 16, "%d\n", dis(gen));
          out << buffer;
        }
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  time_end(begin, "Create random QQ %d", n);
}

using QQFrequencyDict = std::unordered_map<uint64_t, uint64_t>;
using QQFrequency = std::pair<uint64_t, uint64_t>;

template <typename CountsHandler>
void count_qq_partially(const char* file, CountsHandler& handler) {
  auto begin = time_begin();

  std::ifstream in(file);
  uint64_t qq;
  while (in >> qq) {
    // dump data to handler
    handler.handle(qq);
  }
  handler.flush();

  time_end(begin, "Counting Phase 1");
}

class QQStorageManager {
  const int _n;

 public:
  explicit QQStorageManager(int n) : _n(n) {}

  inline int size() const { return _n; }

  inline std::string getStorageName(int i) const {
    return "qq_" + std::to_string(i) + ".data";
  }
};

class QQStorage {
  std::vector<std::ofstream> _outs;
  QQStorageManager _manager;
  int _times;

 public:
  QQStorage(const QQStorageManager& manager) : _manager(manager), _times(0) {
    for (int i = 0; i < _manager.size(); i++) {
      _outs.emplace_back(_manager.getStorageName(i));
    }
  }

  inline int size() const { return _manager.size(); }

  void storage(const std::vector<QQFrequencyDict>& dicts) {
    auto dump_begin = time_begin();
    for (int i = 0; i < _manager.size(); i++) {
      for (const auto& pair : dicts[i]) {
        _outs[i] << pair.first << " " << pair.second << "\n";
      }
    }
    time_end(dump_begin, "Dump %d", ++_times);
  }

 private:
  QQStorage(const QQStorage&);
  QQStorage& operator=(const QQStorage&);
};

class QQFrequencyDataReducer {
  std::vector<std::ifstream> _ins;
  const QQStorageManager _manager;
  const int _top;

 public:
  using TopFrequencyList = std::vector<QQFrequency>;
  using TopIterator = TopFrequencyList::iterator;

  QQFrequencyDataReducer(const QQStorageManager& manager, int top)
      : _manager(manager), _top(top) {
    for (int i = 0; i < _manager.size(); i++) {
      _ins.emplace_back(_manager.getStorageName(i));
    }
  }

  TopFrequencyList reduce() {
    auto begin = time_begin();
    std::vector<TopFrequencyList> results;

    // frequency => qq
    for (int i = 0; i < _manager.size(); i++) {
      results.push_back(reduce(i));
    }

    // conquer sort
    using topItr = std::pair<TopIterator, TopIterator>;
    std::vector<topItr> top_heap;
    TopFrequencyList result;
    for (auto& eachTopResult : results) {
      if (!eachTopResult.empty()) {
        top_heap.emplace_back(eachTopResult.begin(), eachTopResult.end());
      }
    }

    auto topCmp = [](const topItr& o1, const topItr& o2) {
      return (o1.first)->first < (o2.first)->first;
    };
    std::make_heap(top_heap.begin(), top_heap.end(), topCmp);
    for (const auto& top : top_heap) {
      std::cout << (top.first)->first << " " << (top.first)->second
                << std::endl;
    }

    // find Top K !
    int i = 0;
    while ((!top_heap.empty()) && i < _top) {
      std::pop_heap(top_heap.begin(), top_heap.end(), topCmp);
      result.push_back(*(top_heap.back().first));
      (top_heap.back().first)++;
      if (top_heap.back().first == top_heap.back().second) {
        top_heap.pop_back();
      } else {
        std::push_heap(top_heap.begin(), top_heap.end(), topCmp);
      }
      i++;
    }
    time_end(begin, "Reduce");
    return result;
  }

  TopFrequencyList reduce(int i) {
    std::ifstream& in = _ins[i];

    // frequency => qq
    std::unordered_map<uint64_t, uint64_t> frequent_map;
    uint64_t qq;
    uint64_t count;
    while (in >> qq >> count) {
      frequent_map[qq] += count;
    }
    std::priority_queue<std::pair<uint64_t, uint64_t>> heap;
    TopFrequencyList result;
    for (auto& fpair : frequent_map) {
      heap.push(std::make_pair(fpair.second, fpair.first));
      if (frequent_map.size() - heap.size() < _top) {
        result.push_back(heap.top());
        heap.pop();
      }
    }
    std::cout << "Top " << _top << " QQ in file" << _manager.getStorageName(i)
              << std::endl;
    for (const auto& topQQ : result) {
      std::cout << topQQ.second << ": " << topQQ.first << std::endl;
    }
    return result;
  }

 private:
  QQFrequencyDataReducer(const QQFrequencyDataReducer&);
  QQFrequencyDataReducer& operator=(const QQFrequencyDataReducer&);
};

class DumpQQCountsHandler {
  QQStorage _storage;
  std::vector<QQFrequencyDict> _groupQQFrequencyDicts;
  int _size;
  const int kMaxSize = 1 << 16;

 public:
  explicit DumpQQCountsHandler(const QQStorageManager& storageManager)
      : _storage(storageManager),
        _groupQQFrequencyDicts(_storage.size()),
        _size(0) {}

  ~DumpQQCountsHandler() { std::cout << "close.." << std::endl; }

  inline void handle(const uint64_t qq) {
    if (_groupQQFrequencyDicts[qq % _storage.size()][qq]++ == 0) {
      if (++_size > kMaxSize) {
        dump();
      }
    }
  }

  inline void flush() { dump(); }

 private:
  inline void dump() {
    _storage.storage(_groupQQFrequencyDicts);
    for (auto& dict : _groupQQFrequencyDicts) {
      dict.clear();
    }
    _size = 0;
  }

  DumpQQCountsHandler(const DumpQQCountsHandler&);
  DumpQQCountsHandler& operator=(const DumpQQCountsHandler&);
};

int main(int argc, char** argv) {
  const char* qq_file_name = "qq.txt";
  const int kQQNum = 10000000;
  const uint64_t kMaxQQNumber = 100000;
  const int top = 10;

  // 1. create random qq number.
  create_random_qq_datafile(qq_file_name, kQQNum, kMaxQQNumber);

  // 2. counting and storage
  QQStorageManager storageManager(10);
  {
    DumpQQCountsHandler handler(storageManager);
    count_qq_partially("qq.txt", handler);
  }

  // 3. compact and find Topk in each part
  QQFrequencyDataReducer reducer(storageManager, top);
  auto result = reducer.reduce();

  // 4 . output result
  std::cout << "Top " << top << " QQ in all file" << std::endl;
  for (auto& topQQ : result) {
    std::cout << topQQ.second << ": " << topQQ.first << std::endl;
  }
  return 0;
}