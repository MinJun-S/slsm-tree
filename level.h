#include <queue>

#include "run.h"

class Level {
public:
    int max_runs;
    long max_run_size;
    Run** runs_list;                         // next level
    std::deque<Run> runs;               // current level
    Level(int n, long s) : max_runs(n), max_run_size(s) 
    {
       runs_list = new Run* [n];
       for (int i = 0; i < n; i++)
       {
           runs_list[i] = NULL;
       }
    } //max_runs : run ¸î°³ °¡ÁúÁö, max_run size, entry °³¼ö
    //bool remaining(void) const {return max_runs - runs.size();}
};

