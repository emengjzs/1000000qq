// find top K frequent qq numbers in 1000000 qq numbers

#include <algorithm>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <iostream>
#include <memory>
#include <queue>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

inline clock_t time_begin() { return std::clock(); }

template <typename... Args>
inline void time_end(const clock_t begin, const char* message, Args&&... args) {
  printf("Task:");
  printf(message, args...);
  printf(" -- %.2fs\n", double(std::clock() - begin) / CLOCKS_PER_SEC);
}

void create_random_qq_datafile(const char* file, const int n) {
  auto begin = time_begin();
  const int kMaxQQNumber = 100000;
  std::ofstream out(file);
  srand(time(NULL));
  for (int i = 0; i < n; i++) {
    out << rand() % kMaxQQNumber << "\n";
  }
  time_end(begin, "Create random QQ %d", n);
}

template <typename CountsHandler>
void count_qq_partially(const char* file, CountsHandler& handler) {
  const int kMaxMapSize = 10000;
  std::ifstream in(file);
  uint64_t qq;
  std::unordered_map<uint64_t, uint64_t> qq_count_map;
  while (in >> qq) {
    qq_count_map[qq]++;
    if (qq_count_map.size() > kMaxMapSize) {
      // dump data to handler
      for (auto& pair : qq_count_map) {
        handler.handle(pair.first, pair.second);
      }
      qq_count_map.clear();
    }
  }
  for (auto& pair : qq_count_map) {
    handler.handle(pair.first, pair.second);
  }
}

class QQStorageManager {
  int _n;

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

 public:
  QQStorage(const QQStorageManager& manager) : _manager(manager) {
    for (int i = 0; i < _manager.size(); i++) {
      _outs.emplace_back(_manager.getStorageName(i));
    }
  }

  void storage(uint64_t qq, uint64_t count) {
    _outs[qq % _outs.size()] << qq << " " << count << "\n";
  }

 private:
  QQStorage(const QQStorage&);
  QQStorage& operator=(const QQStorage&);
};

class QQFrequencyDataReducer {
  std::vector<std::ifstream> _ins;
  QQStorageManager _manager;
  const int _top;

 public:
  using TopFrequencyList = std::vector<std::pair<uint64_t, uint64_t>>;
  using TopIterator = TopFrequencyList::iterator;

  QQFrequencyDataReducer(const QQStorageManager& manager, int top)
      : _manager(manager), _top(top) {
    for (int i = 0; i < _manager.size(); i++) {
      _ins.emplace_back(_manager.getStorageName(i));
    }
  }

  TopFrequencyList reduce() {
    std::vector<TopFrequencyList> results;
    // file[] : (count => qq)
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

    auto topCmp = [](topItr o1, topItr o2) {
      return o2.first->first < o1.first->first;
    };
    std::make_heap(top_heap.begin(), top_heap.end(), topCmp);

    // find Top K !
    int i = 0;
    while ((!top_heap.empty()) && i < _top) {
      result.push_back(*(top_heap.back().first));
      top_heap.back().first++;
      if (top_heap.back().first == top_heap.back().second) {
        std::pop_heap(top_heap.begin(), top_heap.end(), topCmp);
        top_heap.pop_back();
      } else {
        std::push_heap(top_heap.begin(), top_heap.end(), topCmp);
      }
      i++;
    }
    return result;
  }

  TopFrequencyList reduce(int i) {
    std::ifstream& in = _ins[i];
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
    for (auto& topQQ : result) {
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

 public:
  explicit DumpQQCountsHandler(const QQStorageManager& storageManager)
      : _storage(storageManager) {}
  ~DumpQQCountsHandler() { std::cout << "close.." << std::endl; }

  inline void handle(uint64_t qq, uint64_t count) {
    _storage.storage(qq, count);
  }

 private:
  DumpQQCountsHandler(const DumpQQCountsHandler&);
  DumpQQCountsHandler& operator=(const DumpQQCountsHandler&);
};

int main(int argc, char** argv) {
  const char* qq_file_name = "qq.txt";
  const int qq_num = 10000000;
  const int top = 10;

  // 1. create random qq number.
  create_random_qq_datafile(qq_file_name, qq_num);

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