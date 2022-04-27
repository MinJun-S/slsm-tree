/**
   태현 수정 ver
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>

#include "rtree.h"

#define MAX_DATA			1000000
#define	MAX_FILENAME		100
#define	RANGE_QUERY			1
#define	KNN_QUERY			0

// RTREEMBR rects[] = {
//     { {0, 0, 0, 2, 2, 0} },  /* xmin, ymin, zmin, xmax, ymax, zmax (for 3 dimensional RTree) */
//     { {5, 5, 0, 7, 7, 0} },
//     { {8, 5, 0, 9, 6, 0} },
//     { {7, 1, 0, 9, 2, 0} }
// };


// int nrects = sizeof(rects) / sizeof(rects[0]);
// RTREEMBR search_rect = {
//     {6, 4, 0, 10, 6, 0}   /* search will find above rects that this one overlaps */
// };

int count = 0;

int MySearchCallback(int id, void* arg) 
{
    /* Note: -1 to make up for the +1 when data was inserted */
    //fprintf (stdout, "Hit data mbr %d ", id-1);
    return 1; /* keep going */
}

int main(int argc, char** argv)
{
    int query;
    double x, y, r = 0.0;
    int k = 0;
    char fileName[MAX_FILENAME];
    FILE* fp;
    time_t start, end;
    int num = 0, visited = 0;
    RTREEMBR rect = {}, q = {};

	if (argc != 2)
	{
		printf("Usage: %s [file name] \n", argv[0]);
		printf("query type:\n\t0 (range query)\n\t1 (knn query)\n");
		return -1;
	}
	strcpy(fileName, argv[1]);
	fp = fopen(fileName, "r");
	if (!fp)
	{
		printf("No files of name %s... Ending program...\n", fileName);
		return -1;
	}

	/* Build R-Tree */
	double nx, ny;
	RTREENODE* root = RTreeCreate();			   //<----------------------- make root

	printf("\n * MAXCARD = %d", MAXCARD);
	printf("\n * sizeof(int) = %d", sizeof(int));
	printf("\n * sizeof(RTREEBRANCH) = %d", sizeof(RTREEBRANCH));

	printf("\n* Start r-Tree Build");
	while (fscanf(fp, "p %lf %lf\n", &nx, &ny) != EOF)
	{
		RTREEMBR p = { {nx, ny, 0, nx, ny, 0} };
		RTreeInsertRect(&p, ++num, &root, 0);      //<----------------------- 1. 한줄씩 읽어들이기 시작 data insert
		// 위에 얘 들어가서 < if 특정 level이면> 한줄 추가해서 그때부터 I/O check 하면 될듯
	}
	printf("\n* Success r-Tree Build");
	

	printf("\n * I/O Chek = %d", IO_chek);


	// 아마 Tree print해볼 수 있는 것(내가추가)
	//RTreePrintNode(root, root->level);




	printf("\n\n Finish.\n ");  //쿼리는 일단 주석했다

	printf("\n\nPlease input query type :1(=range) or 0(=kNN) => ");
	scanf("%d", &query);
	//query = atoi(argv[2]);
	

	printf("\nQuery point (ex. x y ) : ");
	scanf("%lf", &x);
	scanf("%lf", &y);
	if (query != RANGE_QUERY && query != KNN_QUERY)
	{
		printf("No valid Query for %d... Ending program...\n", query);
		return -1;
	}
	else if (query == RANGE_QUERY)
	{
		printf("Range (radius) of Query: ");
		scanf("%lf", &r);
		rect.bound[0] = (x > r) ? (x - r) : 0;
		rect.bound[1] = (y > r) ? (y - r) : 0;
		//rect.bound[0] = x - r;
		//rect.bound[1] = y - r;
		rect.bound[2] = 0;
		rect.bound[3] = x + r;
		rect.bound[4] = y + r;
		rect.bound[5] = 0;
		q.bound[0] = x;
		q.bound[1] = y;
		q.bound[2] = 0;
		q.bound[3] = x;
		q.bound[4] = y;
		q.bound[5] = 0;
	}
	else if (query == KNN_QUERY)
	{
		printf("K (number of nearest neighbors) of Query: ");
		scanf("%d", &k);
		q.bound[0] = x;
		q.bound[1] = y;
		q.bound[2] = 0;
		q.bound[3] = x;
		q.bound[4] = y;
		q.bound[5] = 0;
	}
	
	if (query == RANGE_QUERY)
	{
		start = clock();
		count = rangeQuery(root, &rect, &q, r, &visited);
		//count = RTreeSearch(root, &rect, NULL, 0);
		end = clock();
		printf("\nRTree Range Query result: %d points.\n", count);
	}
	else if (query == KNN_QUERY)
	{
		start = clock();
		//KNN_QUERY();
		CANDIDATE *resultQ = NULL;
		int hitCount = 0;
		kNNQuery(root, &q, &hitCount, k, &resultQ, &visited);
		end = clock();
		printf("\nRTree KNN Query result: %d points.\n", hitCount);
		while (resultQ)
		{
			CANDIDATE *tmp = resultQ;
			resultQ = resultQ->next;
			printf("Point (%lf, %lf) with distance %lf\n", tmp->node->mbr.bound[0], tmp->node->mbr.bound[1], tmp->distance);
			free(tmp);
		}
	}

	printf("RTree Total execution time: %f\n", (float)(end - start) / CLOCKS_PER_SEC);
	printf("RTree Total objects visited: %d\n", visited);
	printf("Successful... Ending program...\n\n\n");
	
    
	fclose(fp);
	RTreeDestroy(root);
    return 0;
}
