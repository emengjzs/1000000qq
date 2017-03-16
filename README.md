# 1000000qq

Classic Interview Question: Find top K frequent qq numbers in 1000000 (or 100'000'000) qq numbers. Writting in C++11.



## Environment

Windows 10 with Clang 3.9 and MSVC 14.0



## Run

```
clang++ qq1000.cpp && .\a.exe
```



## Ideas


- Use `unordered_map` to count qq frequency.
- Split data into multiple files and termly dump the map to files, use hash function to locate the file where a qq number would save to.
- Reduce each files and get top K qq number pairs, since the data is small enough, just load it all in the memory.
- Use usual Top K solution to get top k qq  in each file.
- Use merge compact to get Top k in these  qq's, stop when we have got k numbers.



## Contribute

Feel free to open a issue or request if there is any mistake\typos\better solution with the question.



## License

This project is licensed under the terms of the MIT License.

