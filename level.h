#include <queue>

#include "run.h"

class Level {
public:
    int max_runs;
    long max_run_size;
    Run** runs_list;
    //std::deque<Run> runs;
    Level(int n, long s) : max_runs(n), max_run_size(s) 
    {
        runs_list = new Run* [n];
    } //max_runs : run � ������, max_run size, entry ����
    //bool remaining(void) const {return max_runs - runs.size();}
};
