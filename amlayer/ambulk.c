#include <stdio.h>
#include "am.h"
#include "pf.h"
#include <math.h>

int min2(int a, int b){
	if(a < b) return a;
	return b;
}

AM_InsertToLeafFoundBulk ( pageBuf, recId, index, header )
char *pageBuf;
int recId;
int index;
AM_LEAFHEADER *header;

{
	int recSize;
	short tempPtr;
	short oldhead;
	recSize = header->attrLength + AM_ss;
	
	header->recIdPtr = header->recIdPtr - AM_si - AM_ss;
	tempPtr = header->recIdPtr;
	
	/* save  the old head of recId list */
	bcopy ( pageBuf + AM_sl + ( index ) *recSize + header->attrLength,
			( char * ) &oldhead, AM_ss );
	
	/* Update the head of recId list to the new recid to be added */
	bcopy ( ( char * )  &tempPtr, pageBuf + AM_sl + ( index  ) *recSize +
			header->attrLength, AM_ss );
	/* Copy the recId*/
	bcopy ( ( char * ) &recId, pageBuf + tempPtr, AM_si );
	/* make the old head of list the second on list */
	bcopy ( ( char * ) &oldhead, pageBuf + tempPtr + AM_si, AM_ss );
}

/* Insert to a leaf given that the key is new */
AM_InsertToLeafNotFoundBulk ( pageBuf, value, recId, index )
char *pageBuf;
char *value;
int recId;
int index;
{
	int recSize;
	short null = AM_NULL;
	int i;
	AM_LEAFHEADER *header;
	bcopy ( pageBuf, header, AM_sl );
	recSize = header->attrLength + AM_ss;
	
	/* Update the header */
	header->keyPtr = header->keyPtr + recSize;
	/* copy the new key */
	bcopy ( value, pageBuf + AM_sl + ( index ) *recSize, header->attrLength );
	/* make the head of list NULL*/
	bcopy ( ( char * ) &null, pageBuf + AM_sl + ( index ) *recSize +
			header->attrLength, AM_ss );
	/* Now insert as if key were old key */
	AM_InsertToLeafFoundBulk ( pageBuf, recId, index, header );
	header->numKeys++;
	bcopy(header,pageBuf,AM_sl);
}

int AM_BulkInt ( int fd, int attrLength, int *pageNumArr, int *minValArr ,int inpLen) {
	
	if ( inpLen <= 0 ) {
		AM_EmptyStack();
		return -1;
	}
	
	int maxkeys = ( PF_PAGE_SIZE - AM_sint - AM_si ) / ( AM_si + attrLength + AM_si + AM_ss );
	if (( maxkeys % 2) != 0) 
		 	maxkeys = maxkeys - 1;
	
	int rootnum;
	int *pageNumArr2 = (int*)malloc(sizeof(int)*(ceil(inpLen/(maxkeys*1.0))));
	int *minValArr2 = (int*)malloc(sizeof(int)*(ceil(inpLen/(maxkeys*1.0))));

	int pno = 0;

	int key_left = inpLen;
	int cur = 0;

	while ( ((key_left >= 2 * maxkeys) || (key_left <= maxkeys)) && (key_left > 0)) {
		int j = cur;
		int pagenum;
		char *pageBuf;
		int errVal = PF_AllocPage ( fd, &pagenum, &pageBuf );
		AM_Check;
		rootnum = pagenum;

		AM_INTHEADER temphead,*tempheader;
		tempheader = &temphead;
		/* fill the header */
		tempheader->pageType = 'i';
		tempheader->attrLength = attrLength;
		tempheader->maxKeys = maxkeys;
		tempheader->numKeys = min2(maxkeys, key_left-1);
		
		bcopy(tempheader,pageBuf,AM_sint);


		if(pno != 0) minValArr2[pno-1] = minValArr[cur];
		pageNumArr2[pno] = pagenum;
		pno++;

		while ( j-cur < maxkeys && j<inpLen) {
			bcopy((char *)&pageNumArr[j],pageBuf + AM_sint + (j-cur)*(AM_si+attrLength) ,AM_si);
			bcopy((char *)&minValArr[j],pageBuf + AM_sint + AM_si + (j-cur)*(AM_si+attrLength) ,attrLength);
			j++;
		}
			bcopy((char *)&pageNumArr[j],pageBuf + AM_sint + (j-cur)*(AM_si+attrLength) ,AM_si);

		key_left -= (maxkeys+1);
		cur += maxkeys+1;
		errVal = PF_UnfixPage(fd,pagenum,TRUE);
		AM_Check;

	}

	int maxkeys2 = ceil ( key_left / 2.0 );
	int maxkeys22 = floor ( key_left / 2.0 );
	int flag = 1;
	
	while ( key_left > 0 ) {
		int j = cur;
		int pagenum;
		char *pageBuf;
		int errVal = PF_AllocPage ( fd, &pagenum, &pageBuf );
		AM_Check;
		rootnum = pagenum;
		
		AM_INTHEADER temphead,*tempheader;
		tempheader = &temphead;
		/* fill the header */
		tempheader->pageType = 'i';
		tempheader->attrLength = attrLength;
		tempheader->maxKeys = maxkeys;
		if(flag==1)
			tempheader->numKeys = maxkeys2;
		else 
			tempheader->numKeys = maxkeys22;
		bcopy(tempheader,pageBuf,AM_sint);


		if(pno != 0) minValArr2[pno-1] = minValArr[cur];
		pageNumArr2[pno] = pagenum;
		pno++;

		while ( j-cur < maxkeys2 && j< inpLen) {
			bcopy((char *)&pageNumArr[j],pageBuf + AM_sint + (j-cur)*(AM_si+attrLength) ,AM_si);
			bcopy((char *)&minValArr[j],pageBuf + AM_sint + AM_si + (j-cur)*(AM_si+attrLength) ,attrLength);
			j++;
		}
			bcopy((char *)&pageNumArr[j],pageBuf + AM_sint + (j-cur)*(AM_si+attrLength) ,AM_si);
		key_left -= (maxkeys2+1);
		cur += maxkeys2+1;
		errVal = PF_UnfixPage(fd,pagenum,TRUE);
		AM_Check;
		flag = 0;
	}
	/* root node not reached */
	if(inpLen > maxkeys+1){
		AM_BulkInt(fd, attrLength, pageNumArr2, minValArr2, pno);
	} else {
			AM_RootPageNum = rootnum;
	}
	AM_EmptyStack();
	return 0;
}

int AM_Bulk( int fd, int inpLen, const struct key_id *inp, int attrLength ) {
	if ( inpLen <= 0 ){
		AM_EmptyStack();
		return -1;
	}
	
	int recnum;
	int rootnum;
	struct key_id keys[inpLen];
	for (recnum = 0; recnum < inpLen; ++recnum)
	{
		keys[recnum].value = inp[recnum].value ;
		keys[recnum].recId = inp[recnum].recId;
	}
	int maxkeys = ( PF_PAGE_SIZE - AM_sint - AM_si ) / ( AM_si + attrLength + AM_si + AM_ss );
	if (( maxkeys % 2) != 0) 
		 	maxkeys = maxkeys - 1;
		
	int  key_no[inpLen];
	key_no[0] = 1;
	int i = 1;
	int	*pageNumArr = (int*) malloc( sizeof(int) * ( ceil( inpLen /( maxkeys*1.0) ) ) );
	int *minValArr  = (int*) malloc( sizeof(int) * ( ceil( inpLen /( maxkeys*1.0) ) ) );
	for ( i = 1; i < inpLen; i++ ) {
		if ( inp[i].value != inp[i - 1].value ) key_no[i] = key_no[i - 1] + 1;

		else key_no[i] = key_no[i - 1];
	}
	
	int tot_keys = key_no[inpLen - 1];
	int key_left = tot_keys;
	
	int cur = 0;
	int pno = 0;
		
	while ( ((key_left >= 2 * maxkeys) || (key_left <= maxkeys)) && (key_left > 0)) {
		int j = cur;
		int pagenum;
		char *pageBuf;
	
		int errVal = PF_AllocPage ( fd, &pagenum, &pageBuf );
		AM_Check;
		rootnum  = pagenum;

		AM_LEAFHEADER head, *header;
		header = &head;
		header->pageType = 'l';
		header->nextLeafPage = AM_NULL_PAGE;
		header->recIdPtr = PF_PAGE_SIZE;
		header->keyPtr = AM_sl;
		header->freeListPtr = AM_NULL;
		header->numinfreeList = 0;
		header->attrLength = attrLength;
		header->numKeys = 0;
		header->maxKeys = maxkeys;
		/*copy the header onto the page */
		bcopy(header,pageBuf,AM_sl);
	

		if(pno != 0) minValArr[pno-1] = inp[cur].value;
		pageNumArr[pno] = pagenum;
		pno++;

		
		while ( ((key_no[j] - key_no[cur]) < maxkeys) && (j<inpLen)) {
			AM_InsertToLeafNotFoundBulk ( pageBuf, (char *)&keys[j].value, keys[j].recId, key_no[j] - key_no[cur] );
			AM_EmptyStack();
			j++;
		}

		key_left -= maxkeys;
		cur += maxkeys;
		errVal = PF_UnfixPage(fd,pagenum,TRUE);
		AM_Check;
	}
	
	int maxkeys2 = ceil ( key_left / 2.0 );
	while ( key_left > 0 ) {
		int j = cur;
		int pagenum;
		char *pageBuf;
		int errVal = PF_AllocPage ( fd, &pagenum, &pageBuf );
		AM_Check;
		rootnum = pagenum;
		
		
		AM_LEAFHEADER head, *header;
		header = &head;
		header->pageType = 'l';
		header->nextLeafPage = AM_NULL_PAGE;
		header->recIdPtr = PF_PAGE_SIZE;
		header->keyPtr = AM_sl;
		header->freeListPtr = AM_NULL;
		header->numinfreeList = 0;
		header->attrLength = attrLength;
		header->numKeys = 0;
		header->maxKeys = maxkeys;
		/* copy the header onto the page */
		bcopy(header,pageBuf,AM_sl);
		
		if(pno != 0) minValArr[pno-1] = inp[cur].value;
		pageNumArr[pno] = pagenum;
		pno++;
		
		while (key_no[j] - key_no[cur] < maxkeys2 && j<inpLen ) {
			AM_InsertToLeafNotFoundBulk ( pageBuf, (char *)&keys[j].value, keys[j].recId, key_no[j] - key_no[cur] );
			j++;
		}
		errVal = PF_UnfixPage(fd,pagenum,TRUE);
		AM_Check;
		key_left -= maxkeys2;
		cur += maxkeys2;
	}
			
	/* root node not reached */
	if(inpLen > maxkeys){
		AM_BulkInt(fd, attrLength, pageNumArr, minValArr,pno);
	} else {
			AM_RootPageNum = rootnum;
	}
	AM_EmptyStack();
	return 0;
}
