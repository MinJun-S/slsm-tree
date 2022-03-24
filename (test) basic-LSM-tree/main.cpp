/*
	실험용 basic LSM 코드 수정본
*/
#include <iostream>
#include <vector>

#include "lsm_tree.h"
#include "sys.h"
#include "unistd.h"

using namespace std;

void command_loop(LSMTree& tree) {
    char command;
    KEY_t key_a, key_b;
    VAL_t val;
    string file_path;
    
    int i = 0;

    FILE* file; 

    /* Ver. 1 */
    cout << "\n* s-LSM Tree building... " << endl;
    file = fopen("SaveFile/Save_File.txt", "r");
    
    if (file == NULL) {
        cout << "\n* Invalid file address." << endl;
        cout << "\n* Please check Valid File address or Input new datasets by using Commend 'i'.\n " << endl;
        //fclose(file);
    }
    else {
        while (!feof(file))
        {
            float x, y; KEY_t l_key;
            fscanf(file, "%f %f %d\n",&x, &y, &l_key);
            val.x = x; val.y = y;
            tree.put(l_key, val);
            //tree.put(make_key(val.x, val.y), val);
            i++;
        }
        cout << "\n* Success s-LSM Tree building with " << i << " point data!" << endl;
        fclose(file);
    }

    ///* Ver. 2 */
    //file = fopen("src/sample_data.txt", "r");

    //if (file == NULL) {
    //    cout << "\n* Invalid file address. Please check Valid File address!" << endl;
    //    fclose(file);
    //}
    //else {
    //    while (!feof(file))
    //    {
    //        char op; float x, y; 
    //        fscanf(file, "%c %f %f\n", &op, &x, &y);
    //        val.x = x; val.y = y;
    //        //tree.put(l_key, val);
    //        tree.put(make_key(val.x, val.y), val);
    //        i++;
    //    }
    //    cout << "\n* Success s-LSM Building with " << i << " point data!" << endl;
    //    fclose(file);
    //}

    while (cin >> command) {
        
        switch (command) {
        case 'p':
            cin >> val.x>>val.y;

            if (val.x < VAL_MIN_X || val.x > VAL_MAX_X || val.y < VAL_MIN_Y || val.y > VAL_MAX_Y){
                //die("Could not insert value " + to_string(val.x) + to_string(val.y)+ ": out of range.");
                cout<<"Could not insert value " + to_string(val.x) +", " + to_string(val.y) + ": out of range.\n";
            } else {
                tree.put(make_key(val.x,val.y), val);
            }

            break;
        case 'g':
            cin >> key_a;
            tree.get(key_a);
            break;
        case 'h':                              // Modified
            cin >> key_a >> key_b;
            tree.range(key_a, key_b);
            break;
        case 'd':
            cin >> key_a;
            tree.del(key_a);
            break;
        case 'f':
            cin.ignore();
            getline(cin, file_path);
            // Trim quotes
            tree.load(file_path.substr(1, file_path.size() - 2));    // Original Source Code ver.
            break;

        case 't':
            tree.print_tree();
            break;

        case 'i':
            cout << "\n* Loading File and Start s-LSM Tree Building... " << endl;            
            FILE * file; 
            file = fopen("src/sample_data.txt", "r");

            if (file == NULL) {
                cout << "\n* Invalid file address. Please check Valid File address!" << endl;
                break;
            }
            else {
                while (!feof(file))        
                {
                    char op; float x, y;
                    fscanf(file, "%c %f %f\n", &op, &x, &y);
                    val.x = x; val.y = y;
                    tree.put(make_key(val.x, val.y), val);
                    i++;
                }
                cout << "\n* Success s-LSM Building with " << i << " point data!" << endl;
                fclose(file);
            }        
			cout << " * I/O Check = " << tree.IO_Check << endl;
            tree.IO_Check = 0;
            break;

        case 's':
            cout << "\n* Saving File... " << endl;
            //tree.save_run();
            tree.save_file();
            cout << "\n* Success Saving!" << endl;
            break;

		case 'a':
			//tree.save_run();
			break;

        case 'r':                                       // range query

            float dist;
            entry_t entry;
            cout << "* Input x, y and range_distance " << endl;
            cin >> val.x >> val.y >> dist;
            entry.val.x = val.x;
            entry.val.y = val.y;
            entry.key = make_key(val.x, val.y);

            KEY_t Lower; KEY_t Upper;
            Lower = make_key(entry.val.x - dist, entry.val.y - dist);
            Upper = make_key(entry.val.x + dist, entry.val.y + dist);
            cout << "  " << endl;

            tree.range_query(entry, dist);
            cout << " * I/O Check = " << tree.IO_Check << endl;
            tree.IO_Check = 0;
            break;
        case 'k':                                       // knn1 query
            //entry_t entry;
            int k;
            cout << "* (kNN1) input x, y and k " << endl;
            cin >> val.x >> val.y >> k;
            entry.val.x = val.x;
            entry.val.y = val.y;
            entry.key = make_key(val.x, val.y);

            tree.KNN_query1(entry, k);
            cout << " * I/O Check = " << tree.IO_Check << endl;
            tree.IO_Check = 0;
            break;

        case 'n':                                       // knn2 query
        //entry_t entry;
            //int k;
            cout << "* (kNN2) input x, y and k " << endl;
            cin >> val.x >> val.y >> k;
            entry.val.x = val.x;
            entry.val.y = val.y;
            entry.key = make_key(val.x, val.y);

            tree.KNN_query2(entry, k);
            cout << " * I/O Check = " << tree.IO_Check << endl;
            tree.IO_Check = 0;
            break;

        default:
            cout << "* Please Insert Valid Command ! " << endl;
            //die("Invalid command.");
        }
    }
}

int main(int argc, char *argv[]) {
    int opt, buffer_num_pages, buffer_max_entries, depth, fanout, num_threads;
    float bf_bits_per_entry;

    buffer_num_pages = DEFAULT_BUFFER_NUM_PAGES;
    depth = DEFAULT_TREE_DEPTH;
    fanout = DEFAULT_TREE_FANOUT;
    num_threads = DEFAULT_THREAD_COUNT;
    bf_bits_per_entry = DEFAULT_BF_BITS_PER_ENTRY;

    while ((opt = getopt(argc, argv, "b:d:f:t:r:")) != -1) {
        switch (opt) {
        case 'b':
            buffer_num_pages = atoi(optarg);            // buffer size (ex ./lsm -b 1 )
            break;
        case 'd':
            depth = atoi(optarg);                       // tree depth (ex ./lsm -d 50 )
            break;
        case 'f':
            fanout = atoi(optarg);
            break;
        case 't':
            num_threads = atoi(optarg);
            break;
        case 'e':                                       // Modified
            bf_bits_per_entry = atof(optarg);
            break;
        default:
            die("Usage: " + string(argv[0]) + " "
                "[-b number of pages in buffer] "
                "[-d number of levels] "
                "[-f level fanout] "
                "[-t number of threads] "
                "[-r bloom filter bits per entry] "
                "<[workload]");
        }
    }


	//printf("\sizeof(entry_t) : %d\n", sizeof(entry_t));
	//buffer_max_entries = buffer_num_pages * getpagesize() / sizeof(entry_t);
	buffer_max_entries = 4096;
	//printf("\nbuffer size : %d\n", buffer_max_entries);

	LSMTree tree(buffer_max_entries, depth, fanout, num_threads, bf_bits_per_entry);
	command_loop(tree);

    return 0;
}
