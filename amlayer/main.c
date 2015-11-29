/* test3.c: tests deletion and scan. */
#include <stdio.h>
#include "am.h"
#include "pf.h"
#include "testam.h"
#include <stdlib.h>

#define MAXRECS	10000	/* max # of records to insert */
#define FNAME_LENGTH 80	/* file name size */


int cmpfunc(const void *a, const void *b){
	return (*(struct key_id *) a).value - (*(struct key_id *) b).value;
}

main()
{
int fd;	/* file descriptor for the index */
char fname[FNAME_LENGTH];	/* file name */
int recnum;	/* record number */
int sd;	/* scan descriptor */
int numrec;	/* # of records retrieved */
int testval;	

	/* init */
	printf("\nInitializing\n");
	PF_Init();

	/* create index */
	printf("\nCreating index\n");
	AM_CreateIndexFile(RELNAME,0,INT_TYPE,sizeof(int));
	
	/* open the index */
	printf("\nOpening index\n");
	sprintf(fname,"%s.0",RELNAME);
	fd = PF_OpenFile(fname);

	printf("\nPreparing input\n");
	int sz = 200;

	struct key_id keys[sz];
	for (recnum = 0; recnum < sz; ++recnum)
	{
		keys[recnum].value = sz-recnum-1;
		keys[recnum].recId = sz-recnum-1;
	}
	
	qsort(keys, sz, 2*sizeof(int), cmpfunc);
	
	
	printf("\nBulk Loading\n");	
	AM_Bulk(fd, sz, keys , sizeof(int) );
	
	printf("\nDeleting odd number records\n");

	int err;
	for (recnum=1; recnum < sz; recnum += 2){
		err = AM_DeleteEntry(fd,INT_TYPE,sizeof(int),(char *)&recnum,
					recnum);
		printf("Deleting entry %d  err : %d\n", recnum, err);
	}
	
	printf("\nModifying Entry:\n");
	int rr = 198;
	AM_ModifyEntry(fd,INT_TYPE,sizeof(int),(char *)&rr,rr,300);
	printf("Modified entry %d  err : %d\n", rr, err);
	numrec= 0;
	
	printf("\nPrint Entire Tree:\n");
	AM_PrintTree(fd, AM_RootPageNum, 'i');
	
	printf("\nSearch Entry:\n");
	int sv = 180;
	char * pageBuf;
	int pageNum, indexPtr;
	int status = AM_Search(fd,'i',sizeof(int),(char *)&sv,&pageNum,&pageBuf,&indexPtr);
	printf("Entry: %d PageNum: %d Index: %d Status: %d\n", sv, pageNum, indexPtr, status);
	
	// destroy everything 
	printf("\nClosing Down\n");
	PF_CloseFile(fd);
	AM_DestroyIndex(RELNAME,0);

	printf("\nTest done!\n");
}
